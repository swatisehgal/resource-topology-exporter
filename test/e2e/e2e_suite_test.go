package e2e_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/swatisehgal/resource-topology-exporter/test/e2e"
)

var _ = BeforeSuite(func() {
	err := e2e.Setup()
	Expect(err).ToNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	err := e2e.Teardown()
	Expect(err).ToNot(HaveOccurred())
})

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Suite")
}
