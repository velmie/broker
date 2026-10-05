package broker

// Message is detached application data. ID is a logical application identifier,
// not a settlement token or a promise of deduplication. Received slices belong
// to the application. Publishing treats all fields as read-only until return.
type Message struct {
	ID      string
	Body    []byte
	Headers []Header
}

// Header preserves name case, duplicate values and bytes. Each adapter documents
// the restrictions of its native representation, including ordering.
type Header struct {
	Name  string
	Value []byte
}
