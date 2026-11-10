// Package k8slease is the single-writer lock held in a Kubernetes Lease object
// (coordination.k8s.io/v1), for Ibsen running as a StatefulSet on block storage such as EBS.
//
// Kubernetes already runs etcd and offers Leases on top of it, so there is no consensus to run
// here: client-go's leader election takes the Lease, renews it every RetryPeriod, and gives it
// up if it cannot renew within RenewDeadline. Another pod may take over once LeaseDuration has
// passed since the last renewal.
//
// It is linked only by cmd/ibsen-k8s. The ibsen binary does not import it, which
// scripts/check-architecture.sh checks, since client-go is a heavy dependency.
package k8slease

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/tcw/ibsen/adapter/driven/locking"
	"github.com/tcw/ibsen/core/port/driven"
)

// Config says which Lease to hold and how.
type Config struct {
	// Name and Namespace name the Lease object.
	Name      string
	Namespace string
	// Identity is this holder's name in the Lease; the pod name, normally.
	Identity string
	// LeaseDuration is how long after the last renewal another pod may take over;
	// RenewDeadline how long the holder keeps trying to renew before it gives up; RetryPeriod
	// how often it tries.
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
	// FenceMargin is how long before the lease could expire this holder stops calling itself
	// the holder (Held), so the store stops taking writes while the lease is still certainly
	// its own.
	FenceMargin time.Duration
	// WaitTimeout bounds how long AcquireLock waits for the lease; zero waits for as long as
	// it takes, which is what a pod replacing another wants.
	WaitTimeout time.Duration
}

// Defaults are the timings client-go's own controllers use.
var Defaults = Config{
	Name:          "ibsen",
	LeaseDuration: 15 * time.Second,
	RenewDeadline: 10 * time.Second,
	RetryPeriod:   2 * time.Second,
	FenceMargin:   5 * time.Second,
}

// Validate refuses timings leader election cannot keep.
func (c Config) Validate() error {
	switch {
	case c.Name == "" || c.Namespace == "" || c.Identity == "":
		return fmt.Errorf("the lease needs a name, a namespace and a holder identity, got %q, %q and %q", c.Name, c.Namespace, c.Identity)
	case c.LeaseDuration <= c.RenewDeadline:
		return fmt.Errorf("the lease duration (%s) must be longer than the renew deadline (%s)", c.LeaseDuration, c.RenewDeadline)
	case c.RenewDeadline <= c.RetryPeriod:
		return fmt.Errorf("the renew deadline (%s) must be longer than the retry period (%s)", c.RenewDeadline, c.RetryPeriod)
	case c.FenceMargin <= 0 || c.FenceMargin >= c.LeaseDuration:
		return fmt.Errorf("the fence margin (%s) must be more than zero and less than the lease duration (%s)", c.FenceMargin, c.LeaseDuration)
	}
	return nil
}

// Lock holds a Lease. It satisfies driven.SingleIbsenWriterLock.
type Lock struct {
	config Config
	client coordinationv1.CoordinationV1Interface
	onLost locking.LeaseLost

	mu        sync.Mutex
	cancel    context.CancelFunc
	done      chan struct{}
	elector   *leaderelection.LeaderElector
	releasing atomic.Bool
	leading   atomic.Bool
	// renewedAt is when the last renewal that succeeded was sent, in UnixNano: the lease runs
	// from then, at the latest
	renewedAt atomic.Int64
}

var _ driven.SingleIbsenWriterLock = &Lock{}

// New builds a Lock that talks to the cluster through client, the coordination API alone:
// the full clientset would link every API group Kubernetes has, and triple the binary. onLost is what to do when the
// lease is lost while held: stop the process, as wiring.IbsenServer.writeLockLost does.
func New(config Config, client coordinationv1.CoordinationV1Interface, onLost locking.LeaseLost) (*Lock, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if onLost == nil {
		onLost = func(reason error) {
			log.Fatal().Err(reason).Msgf("lost the Kubernetes lease %s/%s", config.Namespace, config.Name)
		}
	}
	return &Lock{config: config, client: client, onLost: onLost}, nil
}

// InCluster builds a Lock from the pod's own service account.
func InCluster(config Config, onLost locking.LeaseLost) (*Lock, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("not running in a Kubernetes pod, or no service account mounted: %w", err)
	}
	client, err := coordinationv1.NewForConfig(restConfig)
	if err != nil {
		return nil, err
	}
	return New(config, client, onLost)
}

// PodNamespace is the namespace this pod runs in: POD_NAMESPACE from the downward API, or the
// service account's namespace file.
func PodNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	if content, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return strings.TrimSpace(string(content))
	}
	return ""
}

// PodName is this pod's name: POD_NAME from the downward API, or the hostname, which a pod's
// is by default.
func PodName() string {
	if name := os.Getenv("POD_NAME"); name != "" {
		return name
	}
	name, _ := os.Hostname()
	return name
}

// renewTracker is the Lease lock, noting when a renewal it sent succeeded.
type renewTracker struct {
	resourcelock.Interface
	lock *Lock
}

func (r renewTracker) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	sent := time.Now()
	err := r.Interface.Create(ctx, record)
	if err == nil && record.HolderIdentity == r.lock.config.Identity {
		r.lock.renewedAt.Store(sent.UnixNano())
	}
	return err
}

func (r renewTracker) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	sent := time.Now()
	err := r.Interface.Update(ctx, record)
	if err == nil && record.HolderIdentity == r.lock.config.Identity {
		r.lock.renewedAt.Store(sent.UnixNano())
	}
	return err
}

// ErrWaitTimeout is what Acquire returns when the lease did not come within WaitTimeout.
var ErrWaitTimeout = errors.New("the Kubernetes lease did not become free in time")

// Acquire takes the Lease, waiting for as long as another holder keeps renewing it, up to
// WaitTimeout. It returns once this process is the holder.
func (l *Lock) Acquire() error {
	l.mu.Lock()
	if l.cancel != nil {
		l.mu.Unlock()
		return errors.New("the Kubernetes lease is already being held or waited for")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan struct{})
	started := make(chan struct{})
	lease := &resourcelock.LeaseLock{
		LeaseMeta:  metaFor(l.config),
		Client:     l.client,
		LockConfig: resourcelock.ResourceLockConfig{Identity: l.config.Identity},
	}
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            renewTracker{Interface: lease, lock: l},
		LeaseDuration:   l.config.LeaseDuration,
		RenewDeadline:   l.config.RenewDeadline,
		RetryPeriod:     l.config.RetryPeriod,
		ReleaseOnCancel: true,
		Name:            l.config.Name,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(context.Context) {
				l.leading.Store(true)
				close(started)
			},
			OnStoppedLeading: func() {
				wasLeading := l.leading.Swap(false)
				if wasLeading && !l.releasing.Load() {
					l.onLost(fmt.Errorf("the Kubernetes lease %s/%s could not be renewed within %s, or was taken by another holder",
						l.config.Namespace, l.config.Name, l.config.RenewDeadline))
				}
			},
			OnNewLeader: func(identity string) {
				if identity != l.config.Identity {
					log.Info().Msgf("the Kubernetes lease %s/%s is held by %s; waiting for it", l.config.Namespace, l.config.Name, identity)
				}
			},
		},
	})
	if err != nil {
		l.cancel, l.done = nil, nil
		l.mu.Unlock()
		cancel()
		return err
	}
	l.elector = elector
	done := l.done
	l.mu.Unlock()

	go func() {
		defer close(done)
		elector.Run(ctx)
	}()

	var timeout <-chan time.Time
	if l.config.WaitTimeout > 0 {
		timer := time.NewTimer(l.config.WaitTimeout)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-started:
		return nil
	case <-done:
		return errors.New("leader election stopped before the lease was taken")
	case <-timeout:
		l.ReleaseLock()
		return fmt.Errorf("%w: waited %s for %s/%s", ErrWaitTimeout, l.config.WaitTimeout, l.config.Namespace, l.config.Name)
	}
}

// AcquireLock satisfies the port; Acquire says why it failed.
func (l *Lock) AcquireLock() bool {
	err := l.Acquire()
	if err != nil {
		log.Warn().Err(err).Msg("unable to take the Kubernetes lease")
	}
	return err == nil
}

// Held reports whether this process can still be sure it holds the lease: it is the leader,
// and its last renewal is younger than the lease minus the fence margin. A fencing store
// refuses writes once this is false, which is before another pod could take over.
func (l *Lock) Held() bool {
	if !l.leading.Load() {
		return false
	}
	renewed := l.renewedAt.Load()
	if renewed == 0 {
		return false
	}
	return time.Since(time.Unix(0, renewed)) < l.config.LeaseDuration-l.config.FenceMargin
}

// ReleaseLock stops renewing and gives the lease up, if this process still holds it. client-go
// releases by writing an empty holder over the record it last saw, an update the API server
// refuses if anyone has changed the Lease since, so a lease another holder now owns is never
// overwritten or deleted.
func (l *Lock) ReleaseLock() bool {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.cancel, l.done = nil, nil
	l.mu.Unlock()
	if cancel == nil {
		return false
	}
	wasLeading := l.leading.Load()
	l.releasing.Store(true)
	cancel()
	<-done
	l.releasing.Store(false)
	return wasLeading
}
