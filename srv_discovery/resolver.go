package srv_discovery

//

import (
	"sync"
	//"log"
	"../enums"
	"fmt"
	etcdv3 "go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

/*
type State struct {
    Addresses []Address // Resolved addresses for the target
    // ServiceConfig is the parsed service config; obtained from
    // serviceconfig.Parse.
    ServiceConfig serviceconfig.Config
}

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

// Default shchme
const (
	etcdScheme = "etcdv3"
)

type EtcdResolver struct {
	scheme        string
	EtcdConfig    etcdv3.Config
	EtcdCli       *etcdv3.Client // etcd3 client
	EtcdWatchPath string
	Watcher       *EtcdWatcher
	Clientconn    resolver.ClientConn
	Wg            sync.WaitGroup
	CloseCh       chan struct{} // 关闭 channel
	Logger        *zap.Logger
}

// 共性key的共同前缀
type EtcdKeyDirPath struct {
	RootName       string //root-name
	ServiceName    string //service-name
	ServiceType    enums.ServiceType
	ServiceVersion string //version
}

func GetCommonEtcdKeyPath(config EtcdKeyDirPath) string {
	return fmt.Sprintf("/%s/%s/%s", config.RootName, config.ServiceName, config.ServiceVersion)
}

/*
//注册resovler--客户端
func RegisterResolver(reso_scheme string, etcdConfig etcdv3.Config, config EtcdKeyDirPath,zaplog *zap.Logger) {
	resolver := &EtcdResolver{
		scheme:        reso_scheme, //name--etcdv3
		EtcdConfig:    etcdConfig,
		EtcdWatchPath: GetCommonEtcdKeyPath(config),
		Logger: zaplog,
	}
	//register resovler
	resolver.Register(resolver)
}
*/
// Build returns itself for resolver, because it's both a builder and a resolver.
func (r *EtcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	var err error
	r.EtcdCli, err = etcdv3.New(r.EtcdConfig)
	if err != nil {
		r.Logger.Error("Create etcd client error", zap.String("errmsg", err.Error()))
		return nil, err
	}

	//用来从etcd获取serverlist,并通知Clientconn更新连接池
	r.Clientconn = cc

	//创建watcher（要监听的路径+etcdclient）
	r.Watcher = NewEtcdWatcher(r.EtcdWatchPath, r.EtcdCli, r.Logger)

	//go with a new groutine，从etcd中监控最新的地址变化，并通知clientconn（r.cc.UpdateState(resolver.State{Addresses: addr})）
	r.start()
	return r, nil
}

// Scheme returns the scheme.
func (r *EtcdResolver) Scheme() string {
	return r.scheme
}

// Start Resover return a closeCh, Should call by Builde func()
func (r *EtcdResolver) start() {
	r.Wg.Add(1)
	go func() {
		defer r.Wg.Done()
		addrlist_channel := r.Watcher.Watch()
		for addr := range addrlist_channel {
			//range在channel上,addr为最新的[]resolver.Address
			r.Clientconn.UpdateState(resolver.State{Addresses: addr})
		}
	}()
}

// ResolveNow is a noop for resolver.
func (r *EtcdResolver) ResolveNow(o resolver.ResolveNowOption) {
}

// Close is a noop for resolver.
func (r *EtcdResolver) Close() {
	r.Watcher.Close()
	r.Wg.Wait()
}

/*
func (r *EtcdResolver)PrintConfig(){
	return
	log.Printf("-------- %s --------\n", name)
	log.Printf("Scheme: %s\n", r.scheme)
	log.Printf("EtcdAddres: %v\n", r.EtcdAddrs)
	log.Printf("SrvName: %s\n", r.SrvName)
	log.Printf("SrvVersion: %s\n", r.SrvVersion)
	log.Printf("SrvTTL: %d\n", r.SrvTTL)
	log.Printf("KeyPrifix： %s\n", r.keyPrifix)
	log.Printf("srvAddrsList： %v\n", r.srvAddrsList)
	log.Printf("-------------------\n")
}
*/

//注册resovler--客户端
func RegisterResolver(reso_scheme string, etcdConfig etcdv3.Config, config EtcdKeyDirPath, zaplog *zap.Logger) {
	etcdresolver := &EtcdResolver{
		scheme:        reso_scheme, //name--etcdv3
		EtcdConfig:    etcdConfig,
		EtcdWatchPath: GetCommonEtcdKeyPath(config),
		Logger:        zaplog,
	}
	//register resovler
	resolver.Register(etcdresolver)
}
