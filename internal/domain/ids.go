package domain

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// StableID derives a deterministic identifier from its parts. Identical input
// always yields the identical ID, which is what makes redelivered messages
// recognisable as duplicates. The separator is a NUL byte so ("ab","c") and
// ("a","bc") cannot collide.
func StableID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// NewCorrelationID returns a random 128-bit identifier. One correlation ID is
// minted per collection cycle and follows the data through every later stage.
func NewCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

// LabelsKey renders labels as a canonical "k=v,k=v" string with sorted keys.
func LabelsKey(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(labels[k])
	}
	return sb.String()
}
