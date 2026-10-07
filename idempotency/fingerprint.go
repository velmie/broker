package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/velmie/idempo"

	"github.com/velmie/broker"
)

const defaultOperation = "consume"

func buildFingerprint(ctx context.Context, delivery broker.Delivery, cfg *Config) (idempo.Fingerprint, error) {
	if cfg.FingerprintFunc != nil {
		fp, err := cfg.FingerprintFunc(ctx, delivery)
		if err != nil {
			return fp, failure("fingerprint", "FingerprintFunc", err)
		}
		return fp, nil
	}
	msg := delivery.Message()
	headers, err := hashHeaders(msg.Headers, cfg.FingerprintHeaders)
	if err != nil {
		return idempo.Fingerprint{}, failure("fingerprint", "FingerprintHeaders", err)
	}
	return idempo.Fingerprint{Operation: defaultOperation, Target: delivery.Source(), HeadersHash: headers, BodyHash: hashBody(msg.Body)}, nil
}
func hashBody(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }

func hashHeaders(headers []broker.Header, keys []string) (string, error) {
	if headers == nil || len(keys) == 0 {
		return "", nil
	}
	var b strings.Builder
	for _, key := range keys {
		value, err := selectedText(headers, key)
		if err != nil {
			return "", err
		}
		b.WriteString(strconv.Quote(key))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(value))
		b.WriteByte('\n')
	}
	if b.Len() == 0 {
		return "", nil
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

// Only selected text fields require legacy map-compatible representation.
func selectedText(headers []broker.Header, name string) (string, error) {
	var result string
	found := false
	for _, header := range headers {
		if !strings.EqualFold(header.Name, name) {
			continue
		}
		if header.Name != name || found {
			return "", errors.New("ambiguous selected header name")
		}
		if !utf8.Valid(header.Value) {
			return "", errors.New("selected header value is not UTF-8")
		}
		result = string(header.Value)
		found = true
	}
	return result, nil
}
