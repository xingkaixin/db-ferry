package utils

import (
	"fmt"
	"os"
	"time"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

type ProgressManager struct {
	bar *progressbar.ProgressBar
}

func NewProgressManager(total int64, description string) *ProgressManager {
	options := []progressbar.Option{
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString("rows"),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionThrottle(100 * time.Millisecond),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetRenderBlankState(true),
	}

	if term.IsTerminal(int(os.Stderr.Fd())) {
		options = append(options,
			progressbar.OptionUseANSICodes(true),
			progressbar.OptionClearOnFinish(),
		)
	}

	bar := progressbar.NewOptions64(total, options...)

	return &ProgressManager{bar: bar}
}

func (pm *ProgressManager) Increment() {
	if pm.bar != nil {
		pm.bar.Add(1)
	}
}

func (pm *ProgressManager) SetCurrent(current int64) {
	if pm.bar != nil {
		pm.bar.Set64(current)
	}
}

func (pm *ProgressManager) Finish() {
	if pm.bar != nil {
		pm.bar.Finish()
	}
}
