// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package main

import (
	"github.com/Azure/acr-cli/internal/logger"
	"github.com/Azure/acr-cli/version"
	"github.com/spf13/cobra"
)

const (
	versionLongMessage = `
Prints version information
`
)

func newVersionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Long:  versionLongMessage,
		RunE: func(_ *cobra.Command, _ []string) error {
			log := logger.Get()
			log.Info().
				Str("version", version.Version).
				Str("revision", version.Revision).
				Msg("ACR CLI version information")
			return nil
		},
	}

	return cmd
}
