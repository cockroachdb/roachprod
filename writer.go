package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	isatty "github.com/mattn/go-isatty"
)

var isStdoutTerminal = isatty.IsTerminal(os.Stdout.Fd())

type uiWriter struct {
	buf       bytes.Buffer
	lineCount int
}

func (w *uiWriter) Flush(out io.Writer) error {
	if len(w.buf.Bytes()) == 0 {
		return nil
	}
	w.clearLines(out)

	for _, b := range w.buf.Bytes() {
		if b == '\n' {
			w.lineCount++
		}
	}
	_, err := out.Write(w.buf.Bytes())
	w.buf.Reset()
	return err
}

func (w *uiWriter) Write(b []byte) (n int, err error) {
	return w.buf.Write(b)
}

func (w *uiWriter) clearLines(out io.Writer) {
	fmt.Fprint(out, strings.Repeat("\033[1A\033[2K\r", w.lineCount))
	w.lineCount = 0
}
