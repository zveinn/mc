// Copyright (c) 2015-2022 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"time"

	"github.com/minio/cli"
)

var (
	rmFlag               = "retention-mode"
	rdFlag               = "retention-duration"
	lhFlag               = "legal-hold"
	continueFlag         = "continue"
	md5Flag              = "md5"
	disableMultipartFlag = "disable-multipart"
	recursiveFlag        = "recursive"
	rewindFlag           = "rewind"
	versionIDFlag        = "version-id"
	olderThanFlag        = "older-than"
	newerThanFlag        = "newer-than"
	storageClassFlag     = "storage-class"
	tagsFlag             = "tags"
	encryptFlag          = "encrypt"
	encryptKeyFlag       = "encrypt-key"
	preserveFlag         = "preserve"
	attrFlag             = "attr"
	zipFlag              = "zip"
)

// Collection of mc flags currently supported
var globalFlags = []cli.Flag{
	cli.StringFlag{
		Name:  "config-dir, C",
		Value: mustGetMcConfigDir(),
		Usage: "path to configuration folder",
	},
	cli.BoolFlag{
		Name:  "quiet, q",
		Usage: "disable progress bar display",
	},
	cli.BoolFlag{
		Name:  "no-color",
		Usage: "disable color theme",
	},
	cli.BoolFlag{
		Name:  "json",
		Usage: "enable JSON lines formatted output",
	},
	cli.BoolFlag{
		Name:  "debug",
		Usage: "enable debug output",
	},
	cli.BoolFlag{
		Name:  "insecure",
		Usage: "disable SSL certificate verification",
	},
	cli.StringFlag{
		Name:  "limit-upload",
		Usage: "limits uploads to a maximum rate in KiB/s, MiB/s, GiB/s. (default: unlimited)",
	},
	cli.StringFlag{
		Name:  "limit-download",
		Usage: "limits downloads to a maximum rate in KiB/s, MiB/s, GiB/s. (default: unlimited)",
	},
	cli.DurationFlag{
		Name:   "conn-read-deadline",
		Usage:  "custom connection READ deadline",
		Hidden: true,
		Value:  10 * time.Minute,
	},
	cli.DurationFlag{
		Name:   "conn-write-deadline",
		Usage:  "custom connection WRITE deadline",
		Hidden: true,
		Value:  10 * time.Minute,
	},
}

// Flags common across all I/O commands such as cp, mirror, stat, pipe etc.
var ioFlags = []cli.Flag{
	cli.StringFlag{
		Name:  encryptKeyFlag,
		Usage: "encrypt/decrypt objects (using server-side encryption with customer provided keys)",
	},
}
