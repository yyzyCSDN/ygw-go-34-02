// Package web embeds the operator console page shipped with the
// scheduler.
package web

import _ "embed"

//go:embed web/console.html
var ConsoleHTML []byte
