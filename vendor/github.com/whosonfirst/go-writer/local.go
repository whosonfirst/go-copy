package writer

import (
	"context"
	"errors"
	"github.com/natefinch/atomic"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
)

func init() {
	wr := NewLocalWriter()
	Register("local", wr)
}

type LocalWriter struct {
	Writer
	root      string
	dir_mode  os.FileMode
	file_mode os.FileMode
}

func NewLocalWriter() Writer {

	wr := LocalWriter{
		dir_mode:  0755,
		file_mode: 0644,
	}

	return &wr
}

func (wr *LocalWriter) Open(ctx context.Context, uri string) error {

	u, err := url.Parse(uri)

	if err != nil {
		return err
	}

	root := u.Path
	info, err := os.Stat(root)

	if err != nil {
		return err
	}

	if !info.IsDir() {
		return errors.New("root is not a directory")
	}

	// check for dir/file mode query parameters here

	wr.root = root
	return nil
}

func (wr *LocalWriter) Write(ctx context.Context, path string, fh io.ReadCloser) error {

	abs_path := wr.URI(path)
	abs_root := filepath.Dir(abs_path)

	tmp_file, err := ioutil.TempFile("", filepath.Base(abs_path))

	if err != nil {
		return err
	}

	tmp_path := tmp_file.Name()
	defer os.Remove(tmp_path)

	_, err = io.Copy(tmp_file, fh)

	if err != nil {
		return err
	}

	err = tmp_file.Close()

	if err != nil {
		return err
	}

	err = os.Chmod(tmp_path, wr.file_mode)

	if err != nil {
		return err
	}

	_, err = os.Stat(abs_root)

	if os.IsNotExist(err) {

		err = os.MkdirAll(abs_root, wr.dir_mode)

		if err != nil {
			return err
		}
	}

	return atomic.ReplaceFile(tmp_path, abs_path)
}

func (wr *LocalWriter) URI(path string) string {
	return filepath.Join(wr.root, path)
}
