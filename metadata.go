package broker

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// singleTextHeader serves optional text conventions without changing the raw
// ordered Header representation. Errors identify the configured field, not its
// potentially sensitive input value.
func singleTextHeader(headers []Header, name string) (value string, found bool, err error) {
	for _, header := range headers {
		if !strings.EqualFold(header.Name, name) {
			continue
		}
		if found {
			return "", false, fmt.Errorf("%s: duplicate header", name)
		}
		if !utf8.Valid(header.Value) {
			return "", false, fmt.Errorf("%s: invalid UTF-8", name)
		}
		value, found = string(header.Value), true
	}
	return value, found, nil
}
