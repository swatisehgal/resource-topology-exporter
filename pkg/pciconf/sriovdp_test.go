package pciconf

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/fromanirh/numalign/pkg/fakesysfs"
	"github.com/fromanirh/numalign/pkg/topologyinfo/pcidev"
)

type pcidevTestData struct {
	address  string
	class    string
	vendor   string
	device   string
	resource string
}

func (ptd pcidevTestData) Attrs() map[string]string {
	return map[string]string{
		"class":  "0x" + ptd.class,
		"vendor": "0x" + ptd.vendor,
		"device": "0x" + ptd.device,
	}
}

func (ptd pcidevTestData) AddToTree(t fakesysfs.Tree) {
	t.Add(ptd.address, ptd.Attrs())
}

func TestPCIDeviceTreeTrivial(t *testing.T) {
	data, err := ioutil.ReadFile("../../config/examples/sriovdp-config.json")
	// extracted from the same config file
	fakeDevs := []pcidevTestData{
		{
			address:  "0000:01:00.0",
			class:    "020000",
			vendor:   "8086",
			device:   "154c",
			resource: "intel_sriov_netdevice",
		},
		{
			address:  "0000:02:00.0",
			class:    "020000",
			vendor:   "8086",
			device:   "10ed",
			resource: "intel_sriov_netdevice",
		},
		{
			address:  "0000:03:00.0",
			class:    "020000",
			vendor:   "8086",
			device:   "154c",
			resource: "intel_sriov_dpdk",
		},
		{
			address:  "0000:04:00.0",
			class:    "020000",
			vendor:   "8086",
			device:   "10ed",
			resource: "intel_sriov_dpdk",
		},
		{
			address:  "0000:05:00.0",
			class:    "020000",
			vendor:   "15b3",
			device:   "1018",
			resource: "mlnx_sriov_rdma",
		},
	}

	base, err := ioutil.TempDir("/tmp", "fakesysfs")
	if err != nil {
		t.Errorf("error creating temp base dir: %v", err)
	}
	fs, err := fakesysfs.NewFakeSysfs(base)
	if err != nil {
		t.Errorf("error creating fakesysfs: %v", err)
	}
	t.Logf("sysfs at %q", fs.Base())

	devs := fs.AddTree("sys", "bus", "pci", "devices")
	for _, fakeDev := range fakeDevs {
		fakeDev.AddToTree(devs)
	}

	err = fs.Setup()
	if err != nil {
		t.Errorf("error setting up fakesysfs: %v", err)
	}

	pciDevs, err := pcidev.NewPCIDevices(filepath.Join(fs.Base(), "sys"))
	if err != nil {
		t.Errorf("error in NewPCIDevices: %v", err)
	}

	pci2Res, err := MakePCIResourceMapFromData(data, pciDevs)
	if err != nil {
		t.Errorf("error in MakePCIResourceMapFromData: %v", err)
	}
	t.Logf("PCI->Resource map: %+#v", pci2Res)

	if len(pci2Res) != len(fakeDevs) {
		t.Errorf("Mismatched PCIResourceMap: expected %d items, found %d", len(fakeDevs), len(pci2Res))
	}
	for _, fakeDev := range fakeDevs {
		resName, ok := pci2Res[fakeDev.address]
		if !ok || resName != fakeDev.resource {
			t.Errorf("bad mapping for %q: %q expected %q", fakeDev.address, resName, fakeDev.resource)
		}

	}

	if _, ok := os.LookupEnv("PCICONF_TEST_KEEP_TREE"); ok {
		t.Logf("found environment variable, keeping fake tree")
	} else {
		err = fs.Teardown()
		if err != nil {
			t.Errorf("error tearing down fakesysfs: %v", err)
		}
	}

}
