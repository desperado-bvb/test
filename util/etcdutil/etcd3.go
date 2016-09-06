package etcd

import (
	"time"
	"strings"

	"golang.org/x/net/context"
	"github.com/ngaut/log"
	"github.com/coreos/etcd/clientv3"
)

type Node struct {
	Value  []byte
	Childs map[string] *Node	
}

type Etcd struct {
	client 		*clientv3.Client
	pathPrefix	string
	reqTimeout	time.Duration
	etcdAddrs	string
	ttl		int64
}

func NewEtcd(client *clientv3.Client, pathPrefix string, reqTimeout time.Duration, etcdAddrs string) *Etcd {
	return &Etcd {
		client:		client,
		pathPrefix:	pathPrefix,
		reqTimeout:	reqTimeout,
		etcdddrs:	etcdAddrs,
	}
}

func (e *Ectd) Create(ctx context.Context, key string,  val string, opts ...clientv3.OpOption) error {
	key = KeyWithPreix(e.pathPrefix, key)
	txcResp, err := e.client.KV.Txn(ctx).If(
		notFound(key),
	).Then(
		clientv3.OpPut(key, val, opts...)
	).Commit()
	if err != nil {
		return err
	}

	if !txnResp.Succeeded {
		return KeyExistsError(key)
	}

	return nil
}

func (e *Etcd) Get(ctx context.Context, key string) ([]byte, error) {
	key = KeyWithPrefix(e.pathPrefix, key)
	resp, err := e.client.KV.GET(ctx, key)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, KeyNotFoundError(key)
	} 

	return resp.Kvs[0].Value, nil
}

func (e *Etcd) Update(ctx context.Context, key string, val string, ttl int64)  error {
	key = keyWithPrefix(e.pathPrefix, key)
	
	opts := nil
	if  ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return err
		}

		opts = []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(lcr.ID))}
	}
	
	getResp, err := e.client.KV.GET(ctx, key)
	if err != nil {
		return nil
	}

	originRevision := 0

	if len(getResp.Kvs) == 0 {
		err = s.Create(ctx, key, val, opts)
		if err != nil {
			return err
		}
	} else {
		originRevision = getResp.Kvs[0].ModRevision
	}

	for {
		txnResp, err := e.client.KV.Txn(ctx).IF(
			clientv3.Compare(clientv3.ModRevision(key) , "=", originRevision),
		).Then(
			clientv3.OpPUT(key, val, opts...),
		).Else(
			clientv3.OpGet(key)
		).Commit()
		if err != nil {
			return err
		}

		if !txnResp.Succeeded {
			getResp = (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
			originRevision = getResp.Kvs[0].ModRevision
			glog.Infof("Update of %s failed because of a conflict, going to retry", key)
			continue
		}
	}

	return nil
}

func (e *Etcd) List(ctx context.Context, key string) (*Node, error) {
	key = keyWithPrefix(e.pathPrefix, key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	resp, err := e.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	root = new(Node)
	length := len(key)
	for i, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) <= length {
			continue
		}
		
		keyTail := key[length:]
		tailNode := parseToDirTree(root, keyTail)
		tailNode.Value = kv.Value
	}

	return root, nil
}

func parseToDirTree(root *Node, path string) *Node  {
	pathDirs := string.Split(path)
	current := root
	var next *Node 
	var ok bool

        for _, dir :=  range pathDirs {
        	if current.Childs == nil {
                	current.Childs = make(map[string] *Node)
                }

               	next , ok = current.Childs[dir]
		if !ok {
			current.Childs[dir] = new(Node)
			next = current.Childs[dir] 
		}
		
		current = next
       	}
	
	return current
}

func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func keyWithPrefix(prefix, key string) string {
	if strings.HashPrefix(key, prefix) {
		return key
	}

	return path.Join(prefix, key)
}
