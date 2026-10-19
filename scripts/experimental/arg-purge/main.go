// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/Azure/acr-cli/internal/arg"
)

func main() {
	if err := arg.RunPrototype(context.Background(), os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "ARG prototype failed:", err)
		os.Exit(1)
	}
}
