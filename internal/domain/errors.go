package domain

import (
	"context"
	"errors"
	"fmt"
)

// Category classifies a failure so callers can decide what to do about it
// without string matching. Only some categories are worth retrying.
type Category string

const (
	CategoryTransient      Category = "transient"      // worth retrying: network blip, contention
	CategoryPermanent      Category = "permanent"      // retrying cannot help
	CategoryValidation     Category = "validation"     // input is malformed; never retry
	CategoryAuthentication Category = "authentication" // credentials rejected
	CategoryUnsupported    Category = "unsupported"    // capability or action not available
	CategoryTimeout        Category = "timeout"        // deadline exceeded; retryable
	CategoryDependency     Category = "dependency"     // database/NATS/etc. unavailable; retryable
	CategoryScript         Category = "script"         // extension misbehaved; never a core failure
)

// Retryable reports whether an operation that failed with this category may
// succeed if attempted again unchanged.
func (c Category) Retryable() bool {
	switch c {
	case CategoryTransient, CategoryTimeout, CategoryDependency:
		return true
	}
	return false
}

// Error is the single typed error used across package boundaries.
type Error struct {
	Category Category
	Op       string
	Err      error
}

func (e *Error) Error() string {
	if e.Op == "" {
		return fmt.Sprintf("%s: %v", e.Category, e.Err)
	}
	return fmt.Sprintf("%s: %s: %v", e.Category, e.Op, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Wrap attaches a category to err. A nil err yields nil.
func Wrap(cat Category, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Category: cat, Op: op, Err: err}
}

// Errorf creates a categorised error from a format string.
func Errorf(cat Category, format string, args ...any) error {
	return &Error{Category: cat, Err: fmt.Errorf(format, args...)}
}

// Categorized lets other packages' error types report a category without
// importing this package's Error type.
type Categorized interface{ Category() Category }

// CategoryOf returns the category of err. Untyped errors are treated as
// permanent unless they are context errors: an unknown failure is not assumed
// to heal by itself, which keeps poison messages from looping.
func CategoryOf(err error) Category {
	if err == nil {
		return ""
	}
	var de *Error
	if errors.As(err, &de) {
		return de.Category
	}
	var ce Categorized
	if errors.As(err, &ce) {
		return ce.Category()
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return CategoryTimeout
	case errors.Is(err, context.Canceled):
		return CategoryTransient
	}
	return CategoryPermanent
}

// IsRetryable reports whether err is worth retrying.
func IsRetryable(err error) bool { return CategoryOf(err).Retryable() }
