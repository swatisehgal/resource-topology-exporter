package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/docopt/docopt-go"

	"github.com/swatisehgal/resource-topology-exporter/pkg/exporter"
	"github.com/swatisehgal/resource-topology-exporter/pkg/finder"
)

const (
	// ProgramName is the canonical name of this program
	ProgramName = "resource-topology-exporter"
)

func main() {
	// Parse command-line arguments.
	args, err := argsParse(nil)
	if err != nil {
		log.Fatalf("failed to parse command line: %v", err)
	}

	pci2ResMap, err := loadPCI2ResourceMapping(args.PCIToResourceMapPath)
	if err != nil {
		log.Fatalf("failed to read the PCI -> Resource mapping: %v", err)
	}
	for pciAddr, resName := range pci2ResMap {
		log.Printf("PCI -> Resource mapping: %q -> %q\n", pciAddr, resName)
	}

	// Get new finder instance
	instance, err := finder.NewFinder(args, pci2ResMap)
	if err != nil {
		log.Fatalf("Failed to initialize NfdWorker instance: %v", err)
	}

	crdExporter, err := exporter.NewExporter()
	if err != nil {
		log.Fatalf("Failed to initialize crdExporter instance: %v", err)
	}

	for {
		podResources, err := instance.Scan()
		if err != nil {
			log.Printf("CRI scan failed: %v\n", err)
			continue
		}

		perNumaResources := instance.Aggregate(podResources)
		log.Printf("allocatedResourcesNumaInfo:%v", spew.Sdump(perNumaResources))

		if err = crdExporter.CreateOrUpdate("default", perNumaResources); err != nil {
			log.Fatalf("ERROR: %v", err)
		}

		time.Sleep(args.SleepInterval)
	}
}

func loadPCI2ResourceMapping(confPath string) (map[string]string, error) {
	src, err := os.Open(confPath)
	if err != nil {
		return nil, err
	}
	defer src.Close()

	dec := json.NewDecoder(src)
	// pci address (parent fn) -> resource name
	pci2ResMap := make(map[string]string)
	err = dec.Decode(&pci2ResMap)
	return pci2ResMap, err
}

// argsParse parses the command line arguments passed to the program.
// The argument argv is passed only for testing purposes.
func argsParse(argv []string) (finder.Args, error) {
	args := finder.Args{}
	usage := fmt.Sprintf(`%s.
  Usage:
  %s [--sleep-interval=<seconds>] [--cri-path=<path>] [--watch-namespace=<namespace>] [--sysfs=<mountpoint>] [--pci-to-resource-map=<pcimap>]
  %s -h | --help
  Options:
  -h --help                      Show this screen.
  --cri-path=<path>              CRI Enddpoint file path to use.
                                 [Default: /host-run/containerd/containerd.sock]
  --sleep-interval=<seconds>     Time to sleep between updates. [Default: 3s]
  --watch-namespace=<namespace>  Namespace to watch pods for. Use "" for all namespaces. [Default: ""]
  --sysfs=<mountpoint>           Mount point of the sysfs. [Default: /host-sys]
  --pci-to-resource-map=<pcimap> Mapping (pciaddress: resourcename) configuration path. [Default: pcimap.json]`,
		ProgramName,
		ProgramName,
		ProgramName,
	)

	arguments, _ := docopt.ParseArgs(usage, argv, ProgramName)
	var err error
	// Parse argument values as usable types.
	if ns, ok := arguments["--watch-namespace"].(string); ok {
		args.Namespace = ns
	}
	args.PCIToResourceMapPath = arguments["--pci-to-resource-map"].(string)
	args.SysfsRoot = arguments["--sysfs"].(string)
	args.CRIEndpointPath = arguments["--cri-path"].(string)
	args.SleepInterval, err = time.ParseDuration(arguments["--sleep-interval"].(string))
	if err != nil {
		return args, fmt.Errorf("invalid --sleep-interval specified: %s", err.Error())
	}
	return args, nil
}
