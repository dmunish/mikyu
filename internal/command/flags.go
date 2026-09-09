package command

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func removeFlagAt(args []string, i int, hasValue bool) []string {
	end := i + 1
	if hasValue && end < len(args) {
		end++
	}

	out := make([]string, 0, len(args)-(end-i))
	out = append(out, args[:i]...)
	out = append(out, args[end:]...)
	return out
}

func ExtractOptionalDuration(args []string, flag string) (time.Duration, bool, []string, error) {
	for i := range args {
		if args[i] == flag {
			if i+1 >= len(args) {
				return 0, false, args, fmt.Errorf("missing value for %s", flag)
			}

			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return 0, false, args, fmt.Errorf("invalid duration for %s: %w", flag, err)
			}

			return d, true, removeFlagAt(args, i, true), nil
		}

		if after, ok := strings.CutPrefix(args[i], flag+"="); ok {
			d, err := time.ParseDuration(after)
			if err != nil {
				return 0, false, args, fmt.Errorf("invalid duration for %s: %w", flag, err)
			}

			return d, true, removeFlagAt(args, i, false), nil
		}
	}

	return 0, false, args, nil
}

func ExtractDuration(args []string, flag string, def time.Duration) (time.Duration, []string, error) {
	d, ok, rest, err := ExtractOptionalDuration(args, flag)
	if err != nil {
		return 0, args, err
	}

	if !ok {
		return def, rest, nil
	}

	return d, rest, nil
}

func ExtractOptionalUint32(args []string, flag string) (uint32, bool, []string, error) {
	for i := range args {
		if args[i] == flag {
			if i+1 >= len(args) {
				return 0, false, args, fmt.Errorf("missing value for %s", flag)
			}

			v, err := strconv.ParseUint(args[i+1], 10, 32)
			if err != nil {
				return 0, false, args, fmt.Errorf("invalid uint value for %s: %w", flag, err)
			}

			return uint32(v), true, removeFlagAt(args, i, true), nil
		}

		if after, ok := strings.CutPrefix(args[i], flag+"="); ok {
			v, err := strconv.ParseUint(after, 10, 32)
			if err != nil {
				return 0, false, args, fmt.Errorf("invalid uint value for %s: %w", flag, err)
			}

			return uint32(v), true, removeFlagAt(args, i, false), nil
		}
	}

	return 0, false, args, nil
}

func ExtractUint32(args []string, flag string, def uint32) (uint32, []string, error) {
	v, ok, rest, err := ExtractOptionalUint32(args, flag)
	if err != nil {
		return 0, args, err
	}

	if !ok {
		return def, rest, nil
	}

	return v, rest, nil
}

func ExtractOptionalTime(args []string, flag string) (time.Time, bool, []string, error) {
	for i := range args {
		if args[i] == flag {
			if i+1 >= len(args) {
				return time.Time{}, false, args, fmt.Errorf("missing value for %s", flag)
			}

			t, err := time.Parse(time.RFC3339, args[i+1])
			if err != nil {
				return time.Time{}, false, args, fmt.Errorf("invalid time for %s: expected RFC3339: %w", flag, err)
			}

			return t, true, removeFlagAt(args, i, true), nil
		}

		if after, ok := strings.CutPrefix(args[i], flag+"="); ok {
			t, err := time.Parse(time.RFC3339, after)
			if err != nil {
				return time.Time{}, false, args, fmt.Errorf("invalid time for %s: expected RFC3339: %w", flag, err)
			}

			return t, true, removeFlagAt(args, i, false), nil
		}
	}

	return time.Time{}, false, args, nil
}

func ExtractTime(args []string, flag string, def time.Time) (time.Time, []string, error) {
	t, ok, rest, err := ExtractOptionalTime(args, flag)
	if err != nil {
		return time.Time{}, args, err
	}

	if !ok {
		return def, rest, nil
	}

	return t, rest, nil
}
