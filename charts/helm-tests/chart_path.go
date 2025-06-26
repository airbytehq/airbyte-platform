package helmtests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func DetermineChartPath(chartSubDir string) string {

	if p := os.Getenv("HELM_CHART_PATH"); p != "" {
		return p
	}

	// Try to guess the chart path from the current working directory.
	if cwd, err := os.Getwd(); err == nil {
		cwd = filepath.ToSlash(cwd)
		idx := strings.Index(cwd, "oss/charts")
		if idx != -1 {
			return cwd[:idx+10] + "/" + chartSubDir
		}
	}

	fmt.Fprintf(os.Stderr, "error: couldn't automatically determine chart path. try setting HELM_CHART_PATH environment variable")
	os.Exit(1)
	return ""
}
