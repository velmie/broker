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
	keys   []string
	values map[string][]string
}

func newCarrier(headers []broker.Header, fields []string) *textCarrier {
	c := &textCarrier{values: make(map[string][]string)}
	for _, field := range fields {
		field = strings.ToLower(field)
		if !slices.Contains(c.fields, field) {
			c.fields = append(c.fields, field)
		}
	}
	for _, header := range headers {
		if _, exists := c.values[header.Name]; !exists {
			c.keys = append(c.keys, header.Name)
		}
		c.values[header.Name] = append(c.values[header.Name], string(header.Value))
	}
	return c
}

func (c *textCarrier) Get(key string) string {
	var values []string
	for _, name := range c.keys {
		if strings.EqualFold(name, key) {
			values = append(values, c.values[name]...)
		}
	}
	if len(values) != 1 || !validText([]byte(values[0])) {
		return ""
	}
	return values[0]
}

// Values keeps distinct spellings distinct when Keys presents aliases.
func (c *textCarrier) Values(key string) []string {
	if values, ok := c.values[key]; ok {
		return slices.Clone(values)
	}
	var values []string
	for _, name := range c.keys {
		if strings.EqualFold(name, key) {
			values = append(values, c.values[name]...)
		}
	}
	return values
}
func (c *textCarrier) Set(key, value string) {
	key = strings.ToLower(key)
	if slices.Contains(c.fields, key) && validText([]byte(value)) {
		if _, exists := c.values[key]; !exists {
			c.keys = append(c.keys, key)
		}
		c.values[key] = []string{value}
	}
}
func (c *textCarrier) Keys() []string { return slices.Clone(c.keys) }

func (c *textCarrier) inject(headers []broker.Header) []broker.Header {
	result := make([]broker.Header, 0, len(headers)+len(c.values))
	for _, header := range headers {
		if !slices.Contains(c.fields, strings.ToLower(header.Name)) {
			result = append(result, broker.Header{Name: header.Name, Value: slices.Clone(header.Value)})
		}
	}
	for _, key := range c.Keys() {
		result = append(result, broker.Header{Name: key, Value: []byte(c.Get(key))})
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
