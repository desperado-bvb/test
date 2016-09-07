package file

import (
	"fmt"
	"os"
	"path"
	
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/util/fileutil"
)

type filePipeline struct {
	dir 	string
	size	string
	count	int

	filec 	chan *fileutil.LockedFile
	errc	chan error
	donec	chan struct{}
}

func newFilePipeline(dir string, count int, fileSize int64) *filePipeline {
	fp := &filePipeline {
		dir:	dir,
		size:	fileSize,
		count:	count,
		filec:	make(chan *fileutil.LockedFile),
		errc:	make(chan error, 1),
		donec:	make(chan struct{})
	}

	go fp.run()
	return fp
}

func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <- fp.filec:
	case err = <- fp.errc:
	}

	return f, err
}

func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	fpath := path.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY. fileutil.PrivateFileMode); err != nil {
		return nil, err
	}

	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		log.Errorf("failed to allocate space when creating new wal file (%v)"), err)
		f.Close()
		return nil, err
	}

	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	defer close(fp.errc)
	for {
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}

		select {
		case fp.filec <- f:
		case <- fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
