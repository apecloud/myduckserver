package binlogreplication

import "testing"

func TestIgnorableBinlogEventTypes(t *testing.T) {
	// Header layout: 4-byte timestamp + 1-byte type.
	cases := []struct {
		typ     byte
		ignore  bool
		comment string
	}{
		{0x03, true, "STOP"},
		{0x1b, true, "HEARTBEAT"},
		{0x1c, true, "IGNORABLE"},
		{0x1d, true, "ROWS_QUERY (#359)"},
		{0x02, false, "QUERY must still be classified by the typed switch"},
		{0xff, false, "unknown"},
	}
	for _, tc := range cases {
		if got := isIgnorableHeaderEventType(tc.typ); got != tc.ignore {
			t.Fatalf("type 0x%02x (%s): ignore=%v, want %v", tc.typ, tc.comment, got, tc.ignore)
		}
	}
}


