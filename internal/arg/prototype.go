// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package arg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
)

func RunPrototype(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("arg-purge", flag.ContinueOnError)
	registry := flags.String("registry", "", "registry name or public-cloud login server")
	registryID := flags.String("registry-id", "", "optional registry ARM resource ID override")
	subscription := flags.String("subscription", "", "optional subscription UUID for ARG registry discovery")
	discover := flags.Bool("discover", false, "return raw ARG artifact rows")
	snapshot := flags.String("snapshot", "", "captured discovery JSON or normalized artifact array")
	agoDays := flags.Float64("ago-days", 7, "manifest age threshold in days")
	region := flags.String("endpoint-region", DefaultRegion, "'global' or ARM region")
	version := flags.String("api-version", DefaultAPIVersion, "ARG API version")
	if err := flags.Parse(args); err != nil {
		return err
	}
	options := Options{
		RegistryName: *registry, RegistryID: *registryID, SubscriptionID: *subscription,
		Region: *region, APIVersion: *version,
	}
	if err := options.Validate(); err != nil {
		return err
	}
	modes := 0
	for _, selected := range []bool{*discover, *snapshot != ""} {
		if selected {
			modes++
		}
	}
	if modes > 1 || flags.NArg() != 0 {
		return errors.New("--discover and --snapshot are mutually exclusive; no positional arguments are accepted")
	}
	const day = float64(24 * time.Hour)
	if math.IsNaN(*agoDays) || math.IsInf(*agoDays, 0) || *agoDays < 0 || *agoDays >= float64(math.MaxInt64)/day {
		return errors.New("--ago-days must be finite, nonnegative and within the Go duration range")
	}
	cutoff := time.Now().UTC().Add(-time.Duration(*agoDays * day))
	var rows []artifact
	nativeSnapshot := false
	if *snapshot != "" {
		data, err := os.ReadFile(*snapshot)
		if err != nil {
			return err
		}
		if bytes.HasPrefix(bytes.TrimSpace(data), []byte("[")) {
			if options.RegistryID == "" {
				return errors.New("normalized snapshot arrays require --registry-id; discovery captures can be replayed by registry name")
			}
			if err := json.Unmarshal(data, &rows); err != nil {
				return fmt.Errorf("decode normalized snapshot: %w", err)
			}
		} else {
			var captured Discovery
			if err := json.Unmarshal(data, &captured); err != nil {
				return fmt.Errorf("decode discovery snapshot: %w", err)
			}
			if captured.Mode != "schema-discovery" ||
				options.RegistryID != "" && !strings.EqualFold(captured.RegistryID, options.RegistryID) {
				return errors.New("snapshot must be a discovery capture for the requested registry")
			}
			options.RegistryID = captured.RegistryID
			if _, err := registrySubscription(options.RegistryID); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return fmt.Errorf("invalid snapshot scope: %w", err)
			}
			rows, err = normalizeArtifacts(captured.Data, options.RegistryID)
			if err != nil {
				return err
			}
			nativeSnapshot = true
		}
	} else {
		captured, err := Discover(ctx, options)
		if err != nil {
			return err
		}
		if *discover {
			encoder := json.NewEncoder(output)
			encoder.SetIndent("", "  ")
			return encoder.Encode(captured)
		}
		options.RegistryID = captured.RegistryID
		rows, err = normalizeArtifacts(captured.Data, captured.RegistryID)
		if err != nil {
			return err
		}
	}
	result, err := selectCandidates(rows, options.RegistryID, cutoff)
	if err != nil {
		return err
	}
	if *snapshot == "" {
		result.AgeBasis = "ARG properties.registryArtifact.pushedAt"
	} else if nativeSnapshot {
		result.AgeBasis = "ARG properties.registryArtifact.pushedAt (captured snapshot)"
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
