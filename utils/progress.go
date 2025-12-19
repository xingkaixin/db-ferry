package utils

import (
	"fmt"
	"os"

	"github.com/schollz/progressbar/v3"
	"golang.org/x/term"
)

type ProgressManager struct {
	bar     *progressbar.ProgressBar
	total   int64
	current int64
}

func NewProgressManager(total int64, description string) *ProgressManager {
	return NewProgressManagerWithUnit(total, description, "rows")
}

func NewProgressManagerWithUnit(total int64, description, unit string) *ProgressManager {
	if unit == "" {
		unit = "rows"
	}
	options := []progressbar.Option{
		progressbar.OptionSetDescription(description),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionSetItsString(unit),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetRenderBlankState(true),
	}

	if term.IsTerminal(int(os.Stderr.Fd())) {
		options = append(options,
			progressbar.OptionUseANSICodes(true),
		)
	}

	bar := progressbar.NewOptions64(total, options...)

	return &ProgressManager{bar: bar, total: total}
}

func (pm *ProgressManager) Increment() {
	if pm.bar != nil {
		if pm.total > 0 && pm.current >= pm.total {
			return
		}
		pm.bar.Add(1)
		pm.current++
	}
}

func (pm *ProgressManager) SetCurrent(current int64) {
	if pm.bar != nil {
		if pm.total > 0 {
			if current > pm.total {
				current = pm.total
			}
		}
		pm.bar.Set64(current)
		pm.current = current
	}
}

func (pm *ProgressManager) Finish() {
	if pm.bar != nil {
		pm.bar.Finish()
		pm.bar = nil
	}
}
