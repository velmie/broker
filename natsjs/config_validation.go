package natsjs

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/velmie/broker"
)

const (
	ruleRequired       = "required"
	ruleGreaterThan    = "gt"
	ruleGreaterOrEqual = "gte"
	ruleEqual          = "eq"
	ruleOneOf          = "oneof"
	ruleType           = "type"
	ruleUnique         = "unique"
)

type configErrors []error

func (failures *configErrors) add(field, rule, message string) {
	*failures = append(*failures, &broker.FieldError{
		Field:  field,
		Reason: rule,
		Cause:  errors.New(message),
	})
}

func (failures *configErrors) required(field, value string) {
	if value == "" {
		failures.add(field, ruleRequired, field+" is required")
	}
}

func (failures *configErrors) positive(field string, value int64) {
	if value <= 0 {
		failures.add(field, ruleGreaterThan, fmt.Sprintf("%s must be greater than 0", field))
	}
}

func (failures configErrors) err() error { return errors.Join(failures...) }

func validConfigSubject(subject string, filter bool) bool {
	return utf8.ValidString(subject) && strings.IndexFunc(subject, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) < 0 && validSubject(subject, filter)
}
