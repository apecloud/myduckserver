package catalog

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/sirupsen/logrus"
)

type Comment[T any] struct {
	Text string `json:"text,omitempty"`
	Meta T      `json:"meta,omitempty"` // extra information, e.g. the original MySQL column type, etc.
}

func DecodeComment[T any](encodedOrRawText string) *Comment[T] {
	if !strings.HasPrefix(encodedOrRawText, ManagedCommentPrefix) {
		return NewComment[T](encodedOrRawText)
	}
	encoded := []byte(encodedOrRawText[len(ManagedCommentPrefix):])
	decoded := make([]byte, base64.StdEncoding.DecodedLen(len(encoded)))
	n, err := base64.StdEncoding.Decode(decoded, encoded)
	if err != nil {
		logrus.Warn("failed to decode comment: ", err)
		return NewComment[T](encodedOrRawText)
	}
	decoded = decoded[:n]

	var c Comment[T]
	if err = json.Unmarshal(decoded, &c); err != nil {
		// not a valid JSON, treat it as a raw text
		// may be a legacy comment without meta field
		// or the object is not managed by us
		return NewComment[T](encodedOrRawText)
	}
	return &c
}

func NewComment[T any](text string) *Comment[T] {
	return &Comment[T]{Text: text}
}

func NewCommentWithMeta[T any](text string, meta T) *Comment[T] {
	return &Comment[T]{Text: text, Meta: meta}
}

func (c *Comment[T]) Encode() string {
	jsonData, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return ManagedCommentPrefix + base64.StdEncoding.EncodeToString(jsonData)
}
