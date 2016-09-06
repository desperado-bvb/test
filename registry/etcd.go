package registry

import (
	"path"
	"time"

	etcd "github.com/pingcap/tidb-binlog/util/etcdutil"
	"golang.org/x/net/context"
)

type EctdRegistry struct {
	client		*etcd.Etcd
	reqTimeout	time.Duration
}

func NewEtcdRegistry(client *etcd.Etcd, reqTimeout time.Duration) Registry {
	return &EtcdRegistry {
		client:		client,
		reqTimeout:	reqTimeout,
	}
}

func (r *EtcdRegistry) ctx() (context.Context, context.cancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), r.reqTimeout)
	return ctx, cancel
}

func (r *EtcdRegistry) prefixed(p ...string) string {
	return path.Join(p...)
}

func isEtcdError(err error, code int) bool {
	eerr, ok := err.(etcd.Error)
	return ok && eerr.Code == code
}
