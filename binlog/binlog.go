package binlog

import (
	"io"
	"os"
	"errors"

	"github.com/pingcap/tidb-binlog/util/fileutil"
)

var (
	SegmentSizeBytes int64 = 64 * 1000 * 1000 //64MB
	ErrFileNotFound        = errors.New("file: file not found")
)

type Binlog struct {
	dir		string

	start		*binlogscheme.BinlogOffset
	decoder		*decoder
	readClose	func() error
	
	mu		sync.Mutex
	encoder		*encoder
	
	locks           []*fileutil.LockedFile
	fp		*filePipeline
}

func Create(dirpath string) (*Binlog, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	tmpdirpath := path.Clean(firpath) + ".tmp"
	if fileutil.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}

	if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}

	p := path.Join(tmpdirpath, fileName(0))
	f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		return nil, err
	}

	if err := fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
		return nil, err
	}

	binlog := &Binlog{
		dir:      dirpath,
	}
	binlog.locks = append(binlog.locks, f)
	return binlog.renameFile(tmpdirpath)
}

func Open(dirpath string, offset *binlogscheme.BinlogOffset) (*Binlog, error) {
	return openAtIndex(dirpath, offset, true)
} 

func OpenForRead(dirpath string, offset *binlogscheme.BinlogOffset) (*Binlog, error) {
	return openAtIndex(dirpath, offset, false)
}

func OpenAtIndex(dirpath string, offset *binlogscheme.BinlogOffset, write bool) (*Binlog, error) {
	names, err := readBinlogNames(dirpath)
	if err != nil {
		return nil, err
	}

	nameIndex, ok := searchIndex(names, offset.Index)
	if !ok {
		return nil, ErrFileNotFound
	}

	first := true

	rcs := make([]io.ReadCloser, 0)
	rs  := make([io.Reader, 0])
	ls  := make([]*fileutil.LockedFile, 0)
	for _, name := range names[nameIndex:] {
		p := path.Join(dirpath, name)
		if write {
			l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs..)
				return nil, err
			}

			ls 	= append(ls, l)
			rcs	= append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}

			if first {
				ret, err  = rf.Seek(offset.Offset, os.SEEK_SET)
				if err != nil {
					closeAll(rcs...)
					return nil, err
				}
				
				if ret <= offser.Offset {
					continue
				}

				first = false
			}

			ls 	= append(ls, rf)
			rcs	= append(rcs, rf)
		}

		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error {return closeAll(rcs...)}
	binlog := &Binlog{
		dir : 		dirpath,
		start:  	offset,
		decoder:   	newDecoder(offset, rs...),
		readCloser: 	closer,
		locks:		ls,
	}

	if write {
		binlog.readClose = nil
		if _, err := parseBinlogName(path.Base(binlog.tail().Name())); err != nil {
			closer()
			return nil ,err
		}

		binlog.fp = newFilePipeline(binlog.dir, SegmenntSizeBytes)
	}

	return binlog, nil
}

func (b *Binlog) Read(offset *binglogscheme.BinlogOffset, nums uint64) (ents []binlogscheme.Entry, newOffset *binglogscheme.BinlogOffset, err error)  {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ent &binlogscheme.Entry{}
	decoder := b.decoder

	err = decoder.decode(ent)
	for index := 0; index < nums && err == nil; index++ {
		
		newEnt = binlogscheme.Entry {
			CommitTs:	ent.CommitTs,
			StartTs:	ent.StartTs,
			Size:		ent.Size,
			Payload:	ent.Payload,
		}
		ents = append(ents, newEnt)
		err = decoder.decode(ent)
	}

	newOffset = decoder.lastOffset()

	return 
}

func (b *Binlog) renameBinlog(tmpdirpath string) (*Binlog, error) {

	if err := os.RemoveAll(b.dir); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpdirpath, b.dir); err != nil {
		return nil, err
	}

	b.fp = newFilePipeline(w.dir, SegmentSizeBytes)
	return b, nil
}

func (b *Binlog) tail() *fileutil.LockedFile {
	if len(b.locks) > 0 {
		return b.locks[len(b.locks)-1]
	}
	return nil
}

func closeAll(rcs ...io.ReadCloser) error {
	for -, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}
