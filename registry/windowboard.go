package registry

import (
	"fmt"
	"errors"
	"strconv"

	"github.com/pingcap/tidb-binlog/storage/etcd"
)

const WindowBoardPrefix = "windowBoard"

func (r *EtcdRegistry) GetWindowBoard() (int64, error) {
	ctx, cancel := r.ctx()
	defer cancel()
	resp, err := r.client.Get(ctx, WindowBoardPrefix)
	if err != nil {
		if isEtcdError(err, etcd.ErrorCodeKeyNotFound) {
			// not found
			e := fmt.Sprintf("Window Board not found in etcd, %s, err)
			log.Error(e)
			return nil, errors.New(e)
		}
		return nil, err
	}

	board, err := strconv.Atoi(string(resp))
	if err != nil {
		return 0, err
	}

	return int64(board), nil
}

func (r *EtcdRegistry) UpdateWindowBoard(board int64) error {
	ctx, cancel := r.ctx()
	defer cancel()
	boardStr := fmt.Sprintf("%d", board)
	if _, err := r.client.Update(ctx, WindowBoard, boardStr, 0); err != nil {
		e := fmt.Sprintf("Failed to update Window Board in etcd %v, %v"object, err)
		log.Error(e)
		return errors.New(e)
	}
	return nil
}
