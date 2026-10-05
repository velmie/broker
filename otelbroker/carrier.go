package otelbroker

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/velmie/broker"
)

type textCarrier struct {
	fields []string
	values map[string]string
}

func newCarrier(headers []broker.Header, fields []string) *textCarrier {
	c := &textCarrier{values: make(map[string]string)}
	for _, field := range fields {
		field = strings.ToLower(field)
		if !slices.Contains(c.fields, field) {
			c.fields = append(c.fields, field)
		}
	}
	counts := make(map[string]int)
	for _, header := range headers {
		name := strings.ToLower(header.Name)
		if !slices.Contains(c.fields, name) {
			continue
		}
		counts[name]++
		if counts[name] == 1 && validText(header.Value) {
			c.values[name] = string(header.Value)
		} else {
			delete(c.values, name)
		}
	}
	return c
}

func (c *textCarrier) Get(key string) string { return c.values[strings.ToLower(key)] }
func (c *textCarrier) Set(key, value string) {
	key = strings.ToLower(key)
	if slices.Contains(c.fields, key) && validText([]byte(value)) {
		c.values[key] = value
	}
}
func (c *textCarrier) Keys() []string {
	keys := make([]string, 0, len(c.values))
	for _, field := range c.fields {
		if _, ok := c.values[field]; ok {
			keys = append(keys, field)
		}
	}
	return keys
}

func (c *textCarrier) inject(headers []broker.Header) []broker.Header {
	result := make([]broker.Header, 0, len(headers)+len(c.values))
	for _, header := range headers {
		if !slices.Contains(c.fields, strings.ToLower(header.Name)) {
			result = append(result, broker.Header{Name: header.Name, Value: slices.Clone(header.Value)})
		}
	}
	for _, key := range c.Keys() {
		result = append(result, broker.Header{Name: key, Value: []byte(c.values[key])})
	}
	return result
}

func validText(value []byte) bool {
	if !utf8.Valid(value) {
		return false
	}
	for _, r := range string(value) {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
