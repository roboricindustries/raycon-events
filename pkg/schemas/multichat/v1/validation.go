package multichat

import "errors"

type ValidationIssue struct{ Field, Reason string }

type ValidationError struct{ Issues []ValidationIssue }

var ErrInvalidContract = errors.New("invalid contract")

func (e *ValidationError) Error() string { return ErrInvalidContract.Error() }
func (e *ValidationError) add(f, r string) {
	e.Issues = append(e.Issues, ValidationIssue{Field: f, Reason: r})
}
func (e *ValidationError) Is(target error) bool { return target == ErrInvalidContract }
