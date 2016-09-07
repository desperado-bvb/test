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
		encoder:  newEncoder(f, 0),
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

			ls 	= append(ls, nil)
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
		if index, err := parseBinlogName(path.Base(binlog.tail().Name())); err != nil {
			closer()
			return nil ,err
		}

		binlog.fp = newFilePipeline(binlog.dir, int(index)+1, SegmenntSizeBytes)
	}

	return binlog, nil
}

func (b *Binlog) Read(offset *binglogscheme.BinlogOffset, nums uint64) (ents []binlogscheme.Entry, err error)  {
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
			Offset:		ent.Offset,
		}
		ents = append(ents, newEnt)
		err = decoder.decode(ent)
	}

	return 
}

func (wb *Binlog) Save(ents []binlogscheme.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(ents) == 0 {
		return nil
	}

	for i := range ents {
		if err := b.saveEntry(&ents[i]); err != nil {
			return err
		}
	}

	curOff, err := b.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	if curOff < SegmentSizeBytes {
		return b.sync()
		return nil
	}

	return w.cut()
}

func (b *Binlog) cut() error {
	off, serr := b.tail().Seek(0, os.SEEK_CUR)
	if serr != nil {
		return serr
	}
	if err := b.tail().Truncate(off); err != nil {
		return err
	}
	if err := b.sync(); err != nil {
		return err
	}

	fpath := path.Join(b.dir, BinlogName(b.seq()))

	newTail, err := b.fp.Open()
	if err != nil {
		return err
	}

	b.locks = append(b.locks, newTail)

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	newTail.Close()

	if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return err
	}

	w.locks[len(w.locks)-1] = newTail
	w.encoder = newEncoder(w.tail())

	log.Infof("segmented binlog file %v is created", fpath)
	return nil
}

func (b *Binlog) sync() error {
	if b.encoder != nil {
		if err := b.encoder.flush(); err != nil {
			return err
		}
	}
	start := time.Now()
	err := fileutil.Fdatasync(b.tail().File)

	duration := time.Since(start)
	if duration > warnSyncDuration {
		log.Warningf("sync duration of %v, expected less than %v", duration, warnSyncDuration)
	}

	return err
}

func (b *Binlog) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var smaller int
	found := false

	for i, l := range b.locks {
		lockIndex, err := parseWalName(path.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	if !found && len(b.locks) != 0 {
		smaller = len(b.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if b.locks[i] == nil {
			continue
		}
		b.locks[i].Close()
	}
	b.locks = b.locks[smaller:]

	return nil
}

func (b *Binlog) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.fp != nil {
		b.fp.Close()
		b.fp = nil
	}

	if b.tail() != nil {
		if err := b.sync(); err != nil {
			return err
		}
	}
	for _, l := range b.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			log.Errorf("failed to unlock during closing wal: %s", err)
		}
	}
	return nil
}

func (b *Binlog) renameBinlog(tmpdirpath string) (*Binlog, error) {

	if err := os.RemoveAll(b.dir); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpdirpath, b.dir); err != nil {
		return nil, err
	}

	b.fp = newFilePipeline(w.dir, 0, SegmentSizeBytes)
	return b, nil
}

func (b *Binlog) tail() *fileutil.LockedFile {
	if len(b.locks) > 0 {
		return b.locks[len(b.locks)-1]
	}
	return nil
}

func (b *Binlog) seq() uint64 {
	t := b.tail()
	if t == nil {
		return 0
	}
	seq, err := parseBinlogName(path.Base(t.Name()))
	if err != nil {
		plog.Fatalf("bad binlog name %s (%v)", t.Name(), err)
	}
	return seq
}

func closeAll(rcs ...io.ReadCloser) error {
	for -, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}
