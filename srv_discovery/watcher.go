package srv_discovery

import (
	"encoding/json"
	etcdv3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc/resolver"
	"sync"
	"time"
)

/*
	// from 	"google.golang.org/grpc/resolver"
	type Address struct {
    // Addr is the server address on which a connection will be established.
    Addr string
    // Type is the type of this address.
    Type AddressType
    // ServerName is the name of this address.
    //
    // e.g. if Type is GRPCLB, ServerName should be the name of the remote load
    // balancer, not the name of the backend.
    ServerName string
    // Metadata is the information associated with Addr, which may be used
    // to make load balancing decision.
    Metadata interface{}
}
*/

/*
	//from https://github.com/grpc/grpc-go/blob/master/resolver/resolver.go
	type ClientConn interface {
	// UpdateState updates the state of the ClientConn appropriately.
	UpdateState(State)
	// NewAddress is called by resolver to notify ClientConn a new list
	// of resolved addresses.
	// The address list should be the complete list of resolved addresses.
	//
	// Deprecated: Use UpdateState instead.
	NewAddress(addresses []Address)
	// NewServiceConfig is called by resolver to notify ClientConn a new
	// service config. The service config should be provided as a json string.
	//
	// Deprecated: Use UpdateState instead.
	NewServiceConfig(serviceConfig string)
}

*/

const (
	CHANNEL_SIZE = 64
)

//独立封装watcher
type EtcdWatcher struct {
	Key       string
	Client    *etcdv3.Client
	Ctx       context.Context
	Cancel    context.CancelFunc
	Wg        sync.WaitGroup
	AddrsList []resolver.Address
	WatchCh   etcdv3.WatchChan // watch() RETURN channel
	Logger    *zap.Logger
}

func (w *EtcdWatcher) Close() {
	w.Cancel()
}

//create a etcd watcher,which belongs to etcd resolver
func NewEtcdWatcher(key string, etcdclient *etcdv3.Client, zaploger *zap.Logger) *EtcdWatcher {
	ctx, cancel := context.WithCancel(context.Background())
	watcher := &EtcdWatcher{
		Key:    key,
		Client: etcdclient,
		Ctx:    ctx,
		Cancel: cancel,
		Logger: zaploger,
	}
	return watcher
}

// sync full addrs
func (w *EtcdWatcher) GetAllAddresses() []resolver.Address {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	total_addrlist := []resolver.Address{}

	//get all prefix keys
	getResp, err := w.Client.Get(ctx, w.Key, etcdv3.WithPrefix())

	if err == nil {
		addrs := w.ExtractAddrs(getResp)
		if len(addrs) > 0 {
			for _, saddr := range addrs {
				total_addrlist = append(total_addrlist, resolver.Address{
					Addr:     saddr.AddrInfo,  // Addr 和grpc的resolver中的结构体格式保持一致
					Metadata: &saddr.Metadata, // Metadata is the information associated with Addr, which may be used
				})
			}
		}
	} else {
		w.Logger.Error("Watcher: get all keys withprefix error", zap.String("errmsg", err.Error()))
	}
	return total_addrlist
}

//返回range channel
func (w *EtcdWatcher) Watch() chan []resolver.Address {
	retchannel := make(chan []resolver.Address, CHANNEL_SIZE)
	w.Wg.Add(1)
	go func() {
		defer func() {
			close(retchannel)
			w.Wg.Done()
		}()

		//init
		w.AddrsList = w.GetAllAddresses()
		retchannel <- w.cloneAddresses(w.AddrsList)

		//starting a watching channel
		w.WatchCh = w.Client.Watch(w.Ctx, w.Key, etcdv3.WithPrefix(), etcdv3.WithPrevKV())
		for wresp := range w.WatchCh {
			//block and go range,watching etcd events change
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					jsonobj := EtcdNodeJsonData{}
					err := json.Unmarshal([]byte(ev.Kv.Value), &jsonobj)
					if err != nil {
						w.Logger.Error("Parse node data error", zap.String("errmsg", err.Error()))
						continue
					}
					//generate grpc Address struct
					addr := resolver.Address{Addr: jsonobj.AddrInfo, Metadata: &jsonobj.Metadata}
					if w.addAddr(addr) {
						//if-add-new,return new
						retchannel <- w.cloneAddresses(w.AddrsList)
					}
				case mvccpb.DELETE:
					jsonobj := EtcdNodeJsonData{}
					err := json.Unmarshal([]byte(ev.PrevKv.Value), &jsonobj)
					w.Logger.Info("key", zap.String("prevalue", string(ev.PrevKv.Value)), zap.String("value", string(ev.Kv.Value)))
					w.Logger.Info("value", zap.String("preky", string(ev.PrevKv.Key)), zap.String("Key", string(ev.Kv.Key)))

					w.Logger.Info("value", zap.String("value", string(ev.PrevKv.Value)))
					w.Logger.Info("key", zap.String("key", string(ev.Kv.Key)))
					if err != nil {
						w.Logger.Error("Parse node data error", zap.String("errmsg", err.Error()))
						continue
					}
					addr := resolver.Address{Addr: jsonobj.AddrInfo, Metadata: &jsonobj.Metadata}
					if w.removeAddr(addr) {
						retchannel <- w.cloneAddresses(w.AddrsList)
					}
				}
			}
		}
	}()

	//直接返回一个可以range的带缓冲channel
	return retchannel
}

//get keys from etcdctl response
func (w *EtcdWatcher) ExtractAddrs(etcdresponse *etcdv3.GetResponse) []EtcdNodeJsonData {
	addrs := []EtcdNodeJsonData{}

	//KVS is slice
	if etcdresponse == nil || etcdresponse.Kvs == nil {
		return addrs
	}

	for i := range etcdresponse.Kvs {
		if v := etcdresponse.Kvs[i].Value; v != nil {
			//parse string to node-data
			jsonobj := EtcdNodeJsonData{}
			err := json.Unmarshal(v, &jsonobj)
			if err != nil {
				w.Logger.Error("Parse node data error", zap.String("errmsg", err.Error()))
				continue
			}
			addrs = append(addrs, jsonobj)
		}
	}
	return addrs
}

//
func (w *EtcdWatcher) cloneAddresses(in []resolver.Address) []resolver.Address {
	out := make([]resolver.Address, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i]
	}
	return out
}

// 检查addr是否已存在,如果没存在就增加
func (w *EtcdWatcher) addAddr(addr resolver.Address) bool {
	for _, v := range w.AddrsList {
		if addr.Addr == v.Addr {
			return false
		}
	}
	w.AddrsList = append(w.AddrsList, addr)
	return true
}

// 检查addr是否已存在,如果没存在就删除
func (w *EtcdWatcher) removeAddr(addr resolver.Address) bool {
	for i, v := range w.AddrsList {
		if addr.Addr == v.Addr {
			w.AddrsList = append(w.AddrsList[:i], w.AddrsList[i+1:]...)
			return true
		}
	}
	return false
}
