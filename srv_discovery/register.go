package srv_discovery

import (
	"encoding/json"
	"fmt"
	"time"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/net/context"

	"grpclb2etcd/enums"
)

const (
	G_ROOT_NAME    = "lb_root"
	etcd_split_sep = "/"
)

//dump to JSON VALUE
type EtcdNodeJsonData struct {
	AddrInfo string
	Metadata map[string]string
}

// 注册到etcd中的key-value信息
type RegisterConfig struct {
	EtcdConfig     etcdv3.Config
	RootName       string //root-name
	ServiceName    string //service-name
	ServiceType    enums.ServiceType
	ServiceVersion string //version
	ServiceNodeID  string //node-name
	RandomSuffix   string
	NodeData       EtcdNodeJsonData
	Ttl            time.Duration
	Logger         *zap.Logger
}

type EtcdRegister struct {
	Etcd3Client *etcdv3.Client
	Logger      *zap.Logger
	Key         string //service uniq-key
	Value       string //micro-service ip+port+weight
	Ttl         time.Duration
	Ctx         context.Context
	Cancel      context.CancelFunc
}

func NewEtcdV3Register(config RegisterConfig) (*EtcdRegister, error) {
	client, err := etcdv3.New(config.EtcdConfig)
	if err != nil {
		config.Logger.Error("Create etcdv3 client error", zap.String("errmsg", err.Error()))
		return nil, err
	}

	//check format
	val, err := json.Marshal(config.NodeData)
	if err != nil {
		config.Logger.Error("Create etcdv3 value error", zap.String("errmsg", err.Error()))
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	registry := &EtcdRegister{
		Etcd3Client: client,
		Logger:      config.Logger,
		Ttl:         config.Ttl / time.Second,
		Ctx:         ctx,
		Cancel:      cancel,
		Key:         BuildEtcdKey(config),
		Value:       string(val),
	}
	return registry, nil
}

func BuildEtcdKey(config RegisterConfig) string {
	return fmt.Sprintf("/%s/%s/%s/%s", config.RootName, config.ServiceName, config.ServiceVersion, config.ServiceNodeID)
}

//call etcd api
func (et *EtcdRegister) RegisterByApi() error {
	var err error
	resp, err := et.Etcd3Client.Grant(context.TODO(), int64(et.Ttl))
	if err != nil {
		et.Logger.Error("Register Grant error", zap.String("errmsg", err.Error()))
		return fmt.Errorf("create etcd3 lease failed: %v", err)
	}
	if _, err := et.Etcd3Client.Put(context.TODO(), et.Key, et.Value, etcdv3.WithLease(resp.ID)); err != nil {
		et.Logger.Error("Set key with ttl error", zap.String("key", et.Key), zap.String("leaseid", fmt.Sprintf("%x", resp.ID)), zap.String("errmsg", err.Error()))
		return fmt.Errorf("set service '%s' with ttl to etcd3 failed: %s", et.Key, err.Error())
	}

	//in keepalive,start with a new groutine for loop
	if _, err := et.Etcd3Client.KeepAlive(context.TODO(), resp.ID); err != nil {
		et.Logger.Error("Set key keepalive error", zap.String("key", et.Key), zap.String("leaseid", fmt.Sprintf("%x", resp.ID)), zap.String("errmsg", err.Error()))
		return fmt.Errorf("refresh service '%s' with ttl to etcd3 failed: %s", et.Key, err.Error())
	}
	return nil
}

//call with timer-tick
func (et *EtcdRegister) RegisterByTimer() error {
	//set keep alive fuction
	KeepAliveFunc := func() error {
		resp, err := et.Etcd3Client.Grant(et.Ctx, int64(et.Ttl)) //et.ttl必须转为int64，否则error
		if err != nil {
			et.Logger.Error("Register service error", zap.String("errmsg", err.Error()))
			return err
		}
		//fmt.Println(resp.ID)
		_, err = et.Etcd3Client.Get(et.Ctx, et.Key)
		if err != nil {
			//the first time
			//if err == rpctypes.ErrKeyNotFound {
			if _, err := et.Etcd3Client.Put(et.Ctx, et.Key, et.Value, etcdv3.WithLease(resp.ID)); err != nil {
				et.Logger.Error("Set key with ttl error", zap.String("key", et.Key), zap.String("leaseid", fmt.Sprintf("%x", resp.ID)), zap.String("errmsg", err.Error()))
			}
			//}
			return err
		} else {
			// refresh set to true for not notifying the watcher
			if _, err := et.Etcd3Client.Put(et.Ctx, et.Key, et.Value, etcdv3.WithLease(resp.ID)); err != nil {
				et.Logger.Error("Refresh key lease with ttl error", zap.String("key", et.Key), zap.String("leaseid", fmt.Sprintf("%x", resp.ID)), zap.String("errmsg", err.Error()))
				return err
			}
			et.Logger.Info("Refresh key lease with ttl succ", zap.String("key", et.Key), zap.String("leaseid", fmt.Sprintf("%x", resp.ID)))
		}
		return nil
	}

	err := KeepAliveFunc()
	if err != nil {
		et.Logger.Error("Keep alive ttl error", zap.String("errmsg", err.Error()))
		return err
	}

	ticker := time.NewTicker(et.Ttl / 5 * time.Second)
	for {
		select {
		case <-ticker.C:
			KeepAliveFunc()
		case <-et.Ctx.Done():
			ticker.Stop() //shutdown timer
			if _, err := et.Etcd3Client.Delete(context.Background(), et.Key); err != nil {
				et.Logger.Error("Unregister  error", zap.String("key", et.Key), zap.String("errmsg", err.Error()))
				return err
			}
			return nil
		}
	}

	return nil
}

//RELEASE KEY
func (et *EtcdRegister) UnRegister() error {
	//call ctx.Done,must goes first
	et.Cancel()
	if _, err := et.Etcd3Client.Delete(context.Background(), et.Key); err != nil {
		et.Logger.Error("Unregister key error", zap.String("key", et.Key), zap.String("errmsg", err.Error()))
		return err
	}
	return nil
}
