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
	case "", "y", "t", "1", "on", "yes", "true":
		return true
	}
	return false
}
