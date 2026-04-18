package consts

import (
	"os"
	"path/filepath"
)

func init() {
	home, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	HomeDir = home

	workdir, err := os.Getwd()
	if err != nil {
		workdir = home
	}
	workdir, err = filepath.Abs(workdir)
	if err != nil {
		workdir = home
	}

	DataDir = filepath.Join(workdir, ".tdl")
	LogPath = filepath.Join(DataDir, "log")
	ExtensionsPath = filepath.Join(DataDir, "extensions")
	ExtensionsDataPath = filepath.Join(ExtensionsPath, "data")

	for _, p := range []string{DataDir, ExtensionsPath, ExtensionsDataPath} {
		if err = os.MkdirAll(p, 0o755); err != nil {
			panic(err)
		}
	}
}
