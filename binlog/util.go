package binlog

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-binlog/util/fileutil"
)

var (
	badBinlogName = errors.New("bad file name")
)

func Exist(dirpath string) bool {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return false
	}

	return len(names) != 0
}

func searchIndex(names []string, index uint64) (int, bool) {
	if i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		curIndex, err := parseFileName(name)
		if err != nil {
			log.Errorf("parse correct name should never fail: %v", err)
		}

		if index >= curIndex {
			return i, true
		}
	}

	return -1, false
}

func readBinlogNames(dirpath string) ([]string, error) {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}

	fnames := checkBinlogNames(names)
	if len(fnames) == 0 {
		return nil, ErrFileNotFound
	}

	return fnames, nil
}

func checkBinlogNames(names []string) []string {
	fnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseBinlogName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				log.Warningf("ignored file %v in wal", name)
			}
			continue
		}
		fnames = append(fnames, name)
	}

	return fnames
}

func parseBinlogName(str string) (index uint64, err error) {
	if !strings.HasSuffix(str, ".bl") {
		return 0, badBinlogName
	}

	_, err = fmt.Sscanf(str, "%016d.bl", &index)
	return
}

func fileName(index uint64) string {
	return fmt.Sprintf("%016d.bl", index)
}
