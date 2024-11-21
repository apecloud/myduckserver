package backend

import (
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/dolthub/go-mysql-server/sql"
)

func (h *DuckBuilder) CreatePipe(ctx *sql.Context, subdir string) (string, error) {
	// Create the FIFO pipe
	pipeDir := filepath.Join(h.provider.DataDir(), "pipes", subdir)
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return "", err
	}
	pipeName := strconv.Itoa(int(ctx.ID())) + ".pipe"
	pipePath := filepath.Join(pipeDir, pipeName)
	ctx.GetLogger().Debugln("Creating FIFO pipe for LOAD/COPY operation:", pipePath)
	if err := syscall.Mkfifo(pipePath, 0600); err != nil {
		return "", err
	}
	return pipePath, nil
}
