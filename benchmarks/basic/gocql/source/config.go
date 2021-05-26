package main

import "flag"

type Workload int

const (
	Inserts Workload = iota
	Selects
	Mixed
)

type Config struct {
	nodeAddress string
	workload    Workload
	tasks       int64
	concurency  int64
	batchSize   int64
	noPrepare   bool
}

func readConfig() Config {
	config := Config{}

	flag.StringVar(
		&config.nodeAddress,
		"address",
		"scylla:9042",
		"Address of database node to connect to",
	)

	workload := flag.String(
		"workload",
		"mixed",
		"Type of work to perform (inserts, selects, mixed)",
	)

	flag.Int64Var(
		&config.tasks,
		"tasks",
		1_000_000,
		"Total number of tasks (requests) to perform the during benchmark. In case of mixed workload there will be tasks inserts and tasks selects",
	)

	flag.Int64Var(
		&config.concurency,
		"concurency",
		256,
		"Maximum number of requests performed at once",
	)

	flag.BoolVar(
		&config.noPrepare,
		"no-prepare",
		false,
		"Don't crate tables and insert into them before the benchmark",
	)

	flag.Parse()

	switch *workload {
	case "inserts":
		config.workload = Inserts
	case "selects":
		config.workload = Selects
	case "mixed":
		config.workload = Mixed
	default:
		panic("invalid workload type")
	}

	config.batchSize = int64(256)

	max := func(a, b int64) int64 {
		if a > b {
			return a
		}

		return b
	}

	if config.tasks/config.batchSize < config.concurency {
		config.batchSize = max(1, config.tasks/config.concurency)
	}

	return config
}
