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
		name := strings.ToLower(header.Name)
		if _, exists := c.values[name]; !exists {
			c.keys = append(c.keys, name)
		}
		c.values[name] = append(c.values[name], string(header.Value))
	}
	return c
}

func (c *textCarrier) Get(key string) string {
	for _, value := range c.values[strings.ToLower(key)] {
		if validText([]byte(value)) {
			return value
		}
	}
	return ""
}

func (c *textCarrier) Values(key string) []string {
	return slices.Clone(c.values[strings.ToLower(key)])
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
