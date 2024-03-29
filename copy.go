package copy

import (
	"bytes"
	"context"
	"errors"
	"github.com/whosonfirst/go-reader"
	"github.com/whosonfirst/go-writer"
	"io"
	"io/ioutil"
)

type Copier struct {
	reader  reader.Reader
	writers []writer.Writer
}

func NewCopier(reader reader.Reader, writers ...writer.Writer) (*Copier, error) {

	if len(writers) == 0 {
		return nil, errors.New("No writers")
	}

	cp := &Copier{
		reader:  reader,
		writers: writers,
	}

	return cp, nil
}

func (cp *Copier) Copy(ctx context.Context, uri string) error {

	fh, err := cp.reader.Read(ctx, uri)

	if err != nil {
		return err
	}

	defer fh.Close()

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done_ch := make(chan bool)
	error_ch := make(chan error)

	for _, wr := range cp.writers {

		br := bytes.NewReader(body)
		fh := ioutil.NopCloser(br)

		go func(ctx context.Context, wr writer.Writer, uri string, fh io.ReadCloser) {

			defer fh.Close()

			select {
			case <-ctx.Done():
				return
			default:
				// pass
			}

			err := wr.Write(ctx, uri, fh)

			if err != nil {
				error_ch <- err
			}

			done_ch <- true

		}(ctx, wr, uri, fh)

	}

	remaining := len(cp.writers)

	for remaining > 0 {

		select {
		case <-done_ch:
			remaining -= 1
		case err := <-error_ch:
			return err
		default:
			// pass
		}
	}

	return nil
}
