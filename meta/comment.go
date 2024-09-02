package meta

import (
	"encoding/json"
	"strings"
)

type Comment struct {
	Text string `json:"text,omitempty"`
	Meta string `json:"meta,omitempty"` // extra information, e.g. the original MySQL column type, etc.
}

func DecodeComment(encodedOrRawText string) *Comment {
	var c Comment
	err := json.Unmarshal([]byte(encodedOrRawText), &c)
	if err != nil {
		// not a valid JSON, treat it as a raw text
		// may be a legacy comment without meta field
		// or the object is not managed by us
		return NewComment(encodedOrRawText)
	}
	return &c
}

func NewComment(text string) *Comment {
	return &Comment{Text: text}
}

func NewCommentWithMeta(text, meta string) *Comment {
	return &Comment{Text: text, Meta: meta}
}

func (c *Comment) Encode() string {
	jsonData, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}
	return norm(string(jsonData))
}

func norm(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}
