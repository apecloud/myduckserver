package catalog

import (
	"database/sql/driver"
	"testing"
)

func TestMySQLRandomBytesExecLength(t *testing.T) {
	got, err := mysqlRandomBytesExec([]driver.Value{int64(3)})
	if err != nil {
		t.Fatal(err)
	}
	b, ok := got.([]byte)
	if !ok {
		t.Fatalf("got %T, want []byte", got)
	}
	if len(b) != 3 {
		t.Fatalf("len = %d; want 3", len(b))
	}
}

func TestMySQLRandomBytesExecRejectsZero(t *testing.T) {
	_, err := mysqlRandomBytesExec([]driver.Value{int64(0)})
	if err == nil {
		t.Fatal("expected error for length 0")
	}
}
