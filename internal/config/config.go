package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
)

// Load reads a flat JSON object of flag-name -> value and applies it to every
// flag that wasn't given on the command line, so the command line always wins.
// Call after fs.Parse. Unknown names are an error, a typo in a config file
// should not silently run with defaults.
func Load(fs *flag.FlagSet, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var vals map[string]any
	if err := json.Unmarshal(b, &vals); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	for name, v := range vals {
		if name == "config" {
			continue
		}
		if fs.Lookup(name) == nil {
			return fmt.Errorf("%s: unknown setting %q", path, name)
		}
		if set[name] {
			continue
		}
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case float64:
			s = strconv.FormatFloat(x, 'f', -1, 64) // Sprint would give 3.6e+09
		case bool:
			s = fmt.Sprint(x)
		default:
			return fmt.Errorf("%s: %q must be a string, number or bool", path, name)
		}
		if err := fs.Set(name, s); err != nil {
			return fmt.Errorf("%s: %q: %w", path, name, err)
		}
	}
	return nil
}
