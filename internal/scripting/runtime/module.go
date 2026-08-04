// Package runtime embeds the goja JavaScript interpreter behind a small,
// deliberately narrow interface: compile a script, call an exported function
// with JSON-shaped arguments under a deadline, get JSON back.
//
// What crosses the Go/JavaScript boundary is always a JSON string that is
// parsed (into) or serialised (out of) inside the interpreter. Scripts never
// hold a reference to a Go value, so there is no path from JavaScript to Go
// reflection, and nothing a script does to its arguments can affect Go state.
package runtime

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dop251/goja"
)

// Program is a compiled, immutable script. It is safe to share between
// interpreters running on different goroutines.
type Program struct {
	ID      string
	Exports []string // names of exported functions and constants
	prog    *goja.Program
}

var (
	exportFunc  = regexp.MustCompile(`(?m)^[ \t]*export[ \t]+(?:async[ \t]+)?function\*?[ \t]+([A-Za-z_$][\w$]*)`)
	exportConst = regexp.MustCompile(`(?m)^[ \t]*export[ \t]+(?:const|let|var)[ \t]+([A-Za-z_$][\w$]*)`)
	exportStrip = regexp.MustCompile(`(?m)^([ \t]*)export[ \t]+(async[ \t]+function|function|const|let|var)\b`)
	unsupported = regexp.MustCompile(`(?m)^[ \t]*(export[ \t]+default\b|export[ \t]*\{|export[ \t]*\*|import\b)`)
	midLine     = regexp.MustCompile(`[;}][ \t]+export\b`)
)

// Compile turns script source into a Program.
//
// Scripts use `export function name(...)` and `export const name = ...`. This
// is not an ES module loader: the declarations are rewritten into a function
// scope private to the script and the exported names are returned from it.
// Default exports, export lists and imports are rejected on purpose; scripts
// cannot depend on each other, which keeps them independently testable and
// keeps the dependency surface at zero.
func Compile(id, source string) (*Program, error) {
	if m := unsupported.FindString(source); m != "" {
		return nil, fmt.Errorf("unsupported module syntax %q: use `export function name()` / `export const name`; imports are not available", strings.TrimSpace(m))
	}
	if m := midLine.FindString(source); m != "" {
		return nil, fmt.Errorf("`export` must start a line (one exported declaration per line); found %q", strings.TrimSpace(m))
	}
	var names []string
	seen := map[string]bool{}
	for _, re := range []*regexp.Regexp{exportFunc, exportConst} {
		for _, m := range re.FindAllStringSubmatch(source, -1) {
			if !seen[m[1]] {
				seen[m[1]] = true
				names = append(names, m[1])
			}
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("script exports nothing")
	}
	body := exportStrip.ReplaceAllString(source, "$1$2")
	// The prologue stays on the first line so reported line numbers match the file.
	var ret strings.Builder
	ret.WriteString("\n;return {")
	for i, n := range names {
		if i > 0 {
			ret.WriteByte(',')
		}
		ret.WriteString(n + ":" + n)
	}
	ret.WriteString("};})()")
	wrapped := `(function(){"use strict";` + body + ret.String()

	prog, err := goja.Compile(id+".js", wrapped, true)
	if err != nil {
		return nil, fmt.Errorf("syntax error: %w", err)
	}
	return &Program{ID: id, Exports: names, prog: prog}, nil
}

// HasExport reports whether the script exports name.
func (p *Program) HasExport(name string) bool {
	for _, n := range p.Exports {
		if n == name {
			return true
		}
	}
	return false
}
