package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"

	"github.com/tcw/ibsen/core/port/driver"
	"github.com/tcw/ibsen/wiring"
)

// --lock k8s, through the same composition root `ibsen append` uses, against a fake cluster:
// the lease is taken, the log is written, and closing gives the lease up.
func TestLockK8sBuildsTheLeaseAdapter(t *testing.T) {
	client := fake.NewClientset()
	newClient = func() (coordinationv1.CoordinationV1Interface, error) { return client.CoordinationV1(), nil }
	t.Cleanup(func() { newClient = nil })
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	Register(flags)
	if err := flags.Parse([]string{"--leaseNamespace", "logs", "--leaseHolder", "ibsen-0",
		"--leaseDuration", "1500ms", "--leaseRenewDeadline", "1s", "--leaseRetryPeriod", "100ms", "--leaseFenceMargin", "500ms"}); err != nil {
		t.Fatal(err)
	}

	local, err := wiring.OpenLocal(wiring.LocalParams{RootPath: t.TempDir(), LockMode: LockName, Compression: "none"})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := client.CoordinationV1().Leases("logs").Get(context.Background(), "ibsen", metav1.GetOptions{})
	if err != nil || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != "ibsen-0" {
		t.Fatalf("the lease is %+v, %v; want it held by ibsen-0", lease, err)
	}
	entries := [][]byte{[]byte("written under the lease")}
	if _, err := local.Write(driver.WriteParams{Topic: "t", Entries: &entries}); err != nil {
		t.Fatalf("a write under a held lease was refused: %v", err)
	}
	if err := local.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	lease, _ = client.CoordinationV1().Leases("logs").Get(context.Background(), "ibsen", metav1.GetOptions{})
	if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity == "ibsen-0" {
		t.Error("closing kept the lease")
	}
}
