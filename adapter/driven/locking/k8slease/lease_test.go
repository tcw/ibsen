package k8slease

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	coordinationclient "k8s.io/client-go/kubernetes/typed/coordination/v1"
	k8stesting "k8s.io/client-go/testing"
)

// The adapter against client-go's fake clientset: the Lease API as the API server keeps it,
// with no cluster. Timings are short so the tests are.

// newCluster is the fake clientset with the one API server behaviour leader election rests
// on: an update carrying a resourceVersion other than the stored one is a conflict. A leader
// renews with a blind update and counts on exactly that to never overwrite a lease another
// holder has taken; the fake on its own accepts anything.
func newCluster() *fake.Clientset {
	client := fake.NewClientset()
	gvr := coordinationv1.SchemeGroupVersion.WithResource("leases")
	var mu sync.Mutex
	client.PrependReactor("*", "leases", func(action k8stesting.Action) (bool, runtime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		tracker := client.Tracker()
		// CreateAction and UpdateAction have the same methods, so the verb is what tells them apart
		switch action.GetVerb() {
		case "create":
			obj := action.(k8stesting.CreateAction).GetObject().(*coordinationv1.Lease).DeepCopy()
			obj.ResourceVersion = "1"
			if err := tracker.Create(gvr, obj, obj.Namespace); err != nil {
				return true, nil, err
			}
			return true, obj, nil
		case "update":
			obj := action.(k8stesting.UpdateAction).GetObject().(*coordinationv1.Lease).DeepCopy()
			stored, err := tracker.Get(gvr, obj.Namespace, obj.Name)
			if err != nil {
				return true, nil, err
			}
			current, _ := strconv.Atoi(stored.(*coordinationv1.Lease).ResourceVersion)
			if obj.ResourceVersion != strconv.Itoa(current) {
				return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"},
					obj.Name, errors.New("the object has been modified; please apply your changes to the latest version"))
			}
			obj.ResourceVersion = strconv.Itoa(current + 1)
			// the tracker's own Update refuses a lease it holds as already existing, so the
			// stored one is replaced outright; this is a test double, under the reactor's lock
			if err := tracker.Delete(gvr, obj.Namespace, obj.Name); err != nil {
				return true, nil, err
			}
			if err := tracker.Create(gvr, obj, obj.Namespace); err != nil {
				return true, nil, err
			}
			return true, obj, nil
		}
		return false, nil, nil
	})
	return client
}

func testConfig(identity string) Config {
	return Config{Name: "ibsen", Namespace: "logs", Identity: identity,
		LeaseDuration: 1500 * time.Millisecond, RenewDeadline: 1000 * time.Millisecond,
		RetryPeriod: 100 * time.Millisecond, FenceMargin: 500 * time.Millisecond}
}

func lease(t *testing.T, client kubernetes.Interface) *coordinationv1.Lease {
	t.Helper()
	got, err := client.CoordinationV1().Leases("logs").Get(context.Background(), "ibsen", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func holder(l *coordinationv1.Lease) string {
	if l.Spec.HolderIdentity == nil {
		return ""
	}
	return *l.Spec.HolderIdentity
}

// heldBy makes the Lease look held by someone else, renewed at renewed, for seconds. client-go
// times a lease it did not write from when it first sees it, not from the renew time in it —
// which is what makes it tolerate clock skew — so a lease that has expired is one with a short
// duration.
func heldBy(t *testing.T, client kubernetes.Interface, identity string, renewed time.Time, seconds int32, create bool) {
	t.Helper()
	duration := seconds
	at := metav1.NewMicroTime(renewed)
	spec := coordinationv1.LeaseSpec{HolderIdentity: &identity, LeaseDurationSeconds: &duration, AcquireTime: &at, RenewTime: &at}
	leases := client.CoordinationV1().Leases("logs")
	if create {
		if _, err := leases.Create(context.Background(), &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{Name: "ibsen", Namespace: "logs"}, Spec: spec}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		return
	}
	current := lease(t, client)
	current.Spec = spec
	if _, err := leases.Update(context.Background(), current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newLock(t *testing.T, client coordinationclient.CoordinationV1Interface, config Config, onLost func(error)) *Lock {
	t.Helper()
	lock, err := New(config, client, onLost)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lock.ReleaseLock() })
	return lock
}

func failOnLost(t *testing.T) func(error) {
	return func(err error) { t.Errorf("lease lost: %v", err) }
}

func TestAcquireTakesTheLeaseAndRenewsIt(t *testing.T) {
	client := newCluster()
	lock := newLock(t, client.CoordinationV1(), testConfig("pod-0"), failOnLost(t))
	if err := lock.Acquire(); err != nil {
		t.Fatal(err)
	}
	first := lease(t, client)
	if holder(first) != "pod-0" || !lock.Held() {
		t.Fatalf("holder %q, held %v; want pod-0 holding it", holder(first), lock.Held())
	}
	renewed := first.Spec.RenewTime.Time
	eventually(t, "a renewal", func() bool { return lease(t, client).Spec.RenewTime.After(renewed) })
	if !lock.Held() {
		t.Error("a renewing holder does not call itself the holder")
	}
}

func TestAnotherHoldersLiveLeaseIsWaitedForAndNotTaken(t *testing.T) {
	client := newCluster()
	heldBy(t, client, "pod-1", time.Now(), 15, true)
	config := testConfig("pod-0")
	config.WaitTimeout = 800 * time.Millisecond
	lock := newLock(t, client.CoordinationV1(), config, failOnLost(t))
	if err := lock.Acquire(); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("got %v while pod-1 holds a live lease, want ErrWaitTimeout", err)
	}
	if lock.Held() || holder(lease(t, client)) != "pod-1" {
		t.Fatal("the lease of a live holder was taken")
	}
}

func TestAnExpiredLeaseIsTakenOver(t *testing.T) {
	client := newCluster()
	heldBy(t, client, "pod-1", time.Now().Add(-time.Hour), 1, true)
	lock := newLock(t, client.CoordinationV1(), testConfig("pod-0"), failOnLost(t))
	if err := lock.Acquire(); err != nil {
		t.Fatal(err)
	}
	if holder(lease(t, client)) != "pod-0" {
		t.Fatalf("the expired lease is held by %q", holder(lease(t, client)))
	}
}

// Losing the lease is reported, so wiring can stop the process, and the lock stops calling
// itself the holder, so a fencing store stops taking writes. Releasing afterwards does not
// take back a lease another holder now owns.
func TestLosingTheLeaseIsReportedAndReleaseDoesNotStealIt(t *testing.T) {
	client := newCluster()
	var lost atomic.Int32
	lock := newLock(t, client.CoordinationV1(), testConfig("pod-0"), func(error) { lost.Add(1) })
	if err := lock.Acquire(); err != nil {
		t.Fatal(err)
	}
	// another pod takes it, as one would once this one had stalled past its lease
	heldBy(t, client, "pod-1", time.Now(), 15, false)
	eventually(t, "the loss to be reported", func() bool { return lost.Load() == 1 })
	if lock.Held() {
		t.Error("a holder that lost its lease still calls itself the holder")
	}
	lock.ReleaseLock()
	if got := holder(lease(t, client)); got != "pod-1" {
		t.Fatalf("after release the lease is held by %q, want pod-1 kept", got)
	}
	if lost.Load() != 1 {
		t.Errorf("the loss was reported %d times", lost.Load())
	}
}

func TestReleaseGivesUpALeaseItHolds(t *testing.T) {
	client := newCluster()
	first := newLock(t, client.CoordinationV1(), testConfig("pod-0"), failOnLost(t))
	if err := first.Acquire(); err != nil {
		t.Fatal(err)
	}
	if !first.ReleaseLock() {
		t.Fatal("release of a held lease reported nothing released")
	}
	if got := holder(lease(t, client)); got == "pod-0" {
		t.Fatal("release left this holder in the lease")
	}
	// the next pod need not wait out the lease
	config := testConfig("pod-1")
	config.WaitTimeout = time.Second
	second := newLock(t, client.CoordinationV1(), config, failOnLost(t))
	if err := second.Acquire(); err != nil {
		t.Fatalf("a released lease was not free: %v", err)
	}
}

func TestTimingsLeaderElectionCannotKeepAreRefused(t *testing.T) {
	for _, bad := range []func(*Config){
		func(c *Config) { c.Namespace = "" },
		func(c *Config) { c.RenewDeadline = c.LeaseDuration },
		func(c *Config) { c.RetryPeriod = c.RenewDeadline },
		func(c *Config) { c.FenceMargin = c.LeaseDuration },
	} {
		config := testConfig("pod-0")
		bad(&config)
		if _, err := New(config, newCluster().CoordinationV1(), nil); err == nil {
			t.Errorf("%+v was accepted", config)
		}
	}
}
