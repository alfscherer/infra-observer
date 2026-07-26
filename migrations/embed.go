// Package migrations embeds the SQL migrations so the single binary can apply
// them (`infra-observer migrate`) without shipping loose files.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
