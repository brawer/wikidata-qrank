package main

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

func findEntitiesDump(dumpsPath string) (time.Time, string, error) {
	path := filepath.Join(dumpsPath, "wikidatawiki", "entities", "latest-all.json.bz2")
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return time.Time{}, "", err
	}

	parts := strings.Split(resolved, string(os.PathSeparator))
	date, err := time.Parse("20060102", parts[len(parts)-2])
	if err != nil {
		return time.Time{}, "", err
	}

	// The symlink can change any time on the file system, such as
	// when Wikimedia generates a new dump right between the call
	// to EvalSymlinks() and our client opening the returned path.
	// To avoid this race condition, we need to return the resolved
	// path here, not the symlink.
	return date, resolved, nil
}
