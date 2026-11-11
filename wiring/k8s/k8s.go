// Package k8s is the composition root's Kubernetes half: it registers --lock k8s, the
// single-writer lock held in a Lease object, and the flags that configure it. Only
// cmd/ibsen-k8s imports it, so the ibsen binary links no client-go; scripts/check-architecture.sh
// holds the ibsen binary to that.
package k8s

import (
	"os"
	"time"

	"github.com/spf13/pflag"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"

	"github.com/tcw/ibsen/adapter/driven/locking"
	"github.com/tcw/ibsen/adapter/driven/locking/k8slease"
	"github.com/tcw/ibsen/core/port/driven"
	"github.com/tcw/ibsen/wiring"
)

// LockName is the --lock value for the Kubernetes lease.
const LockName = "k8s"

// Config is the lease the flags describe.
var Config = k8slease.Defaults

// newClient is the cluster the lock talks to: the pod's own, unless a test says otherwise.
var newClient func() (coordinationv1.CoordinationV1Interface, error)

// Register adds --lock k8s, and its flags to fs with an environment variable each. Call it
// before the command line is parsed.
func Register(fs *pflag.FlagSet) {
	Config.Namespace = k8slease.PodNamespace()
	Config.Identity = k8slease.PodName()
	str := func(p *string, name, env, usage string) {
		if value := os.Getenv(env); value != "" {
			*p = value
		}
		fs.StringVar(p, name, *p, usage+" ("+env+")")
	}
	dur := func(p *time.Duration, name, env, usage string) {
		if value, err := time.ParseDuration(os.Getenv(env)); err == nil {
			*p = value
		}
		fs.DurationVar(p, name, *p, usage+" ("+env+")")
	}
	str(&Config.Name, "leaseName", "IBSEN_LEASE_NAME", "--lock k8s: the Lease object's name")
	str(&Config.Namespace, "leaseNamespace", "IBSEN_LEASE_NAMESPACE", "--lock k8s: the Lease object's namespace; default this pod's")
	str(&Config.Identity, "leaseHolder", "IBSEN_LEASE_HOLDER", "--lock k8s: this holder's name in the Lease; default this pod's name")
	dur(&Config.LeaseDuration, "leaseDuration", "IBSEN_LEASE_DURATION", "--lock k8s: how long after the last renewal another pod may take over")
	dur(&Config.RenewDeadline, "leaseRenewDeadline", "IBSEN_LEASE_RENEW_DEADLINE", "--lock k8s: how long the holder keeps trying to renew before it stops")
	dur(&Config.RetryPeriod, "leaseRetryPeriod", "IBSEN_LEASE_RETRY_PERIOD", "--lock k8s: how often the lease is renewed, or tried for")
	dur(&Config.FenceMargin, "leaseFenceMargin", "IBSEN_LEASE_FENCE_MARGIN", "--lock k8s: how long before the lease could expire writes are refused")
	dur(&Config.WaitTimeout, "leaseWaitTimeout", "IBSEN_LEASE_WAIT_TIMEOUT", "--lock k8s: how long to wait for another holder's lease; 0 waits for as long as it takes")
	wiring.RegisterLock(LockName, build)
}

func build(_ string, onLost locking.LeaseLost) (driven.SingleIbsenWriterLock, error) {
	if newClient != nil {
		client, err := newClient()
		if err != nil {
			return nil, err
		}
		return k8slease.New(Config, client, onLost)
	}
	return k8slease.InCluster(Config, onLost)
}
