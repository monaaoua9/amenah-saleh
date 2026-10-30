package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/monaaoua9/amenah-saleh/internal/ctl"
)

const usage = `usage: streamctl [-addr host:port] <command>

  stats                      engine counters
  top [-by sum] [-n 10]      keys ranked by a metric of their newest window
  results [-key k] [-n 20]   recent window results
  ready                      exit 0 when the engine is running`

func main() {
	addr := flag.String("addr", "localhost:8080", "engine -http address")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, usage) }
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	c := ctl.New("http://" + *addr)

	cmd, args := flag.Arg(0), flag.Args()[1:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var err error
	switch cmd {
	case "stats":
		err = c.Stats(os.Stdout)
	case "top":
		by := fs.String("by", "sum", "count sum min max avg stddev p50 p95 p99 distinct")
		n := fs.Int("n", 10, "rows")
		fs.Parse(args)
		err = c.Top(os.Stdout, *by, *n)
	case "results":
		key := fs.String("key", "", "only this key")
		n := fs.Int("n", 20, "rows")
		fs.Parse(args)
		err = c.Results(os.Stdout, *key, *n)
	case "ready":
		err = c.Ready()
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
