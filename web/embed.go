package web

import (
	"embed"
	"io/fs"
)

//go:embed all:dashboard/dist
var distFS embed.FS

func getDistFS() (fs.FS, error) {
	return fs.Sub(distFS, "dashboard/dist")
}
