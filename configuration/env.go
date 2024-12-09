package configuration

import (
	"os"
	"strings"
)

const (
	replicationWithoutIndex = "REPLICATION_WITHOUT_INDEX"
)

func IsReplicationWithoutIndex() bool {
	switch strings.ToLower(os.Getenv(replicationWithoutIndex)) {
	case "", "t", "1", "true":
		return true
	}
	return false
}
