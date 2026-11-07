//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !windows

package fslock

import (
	"errors"
	"os"
)

func lockFile(*os.File) error {
	return errors.New("file lock: locking unsupported on this platform")
}
