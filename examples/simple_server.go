package main

//A SIMPLE HELLO-WORLD SERVER
//author:pandaychen

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"../enums"
	proto "../proto"
	srvdiscovery "../srv_discovery"
	"../utils"
	etcd3 "go.etcd.io/etcd/clientv3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var SERVICE_NAME = "helloworld"

type Server struct {
	Addr    string
	Weight  int
	Grpcsrv *grpc.Server
}

func NewRpcServer(addr string) *Server {
	srv := grpc.NewServer()
	rs := &Server{
		Addr:    addr,
		Grpcsrv: srv,
	}
	return rs
}

func (s *Server) Run() {
	listener, err := net.Listen("tcp", s.Addr)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}
	log.Printf("rpc listening on:%s", s.Addr)

	proto.RegisterTestServer(s.Grpcsrv, s)
	s.Grpcsrv.Serve(listener)
}

func (s *Server) GracefulStop() {
	s.Grpcsrv.GracefulStop()
}

func (s *Server) Say(ctx context.Context, req *proto.SayReq) (*proto.SayResp, error) {
	text := "Hello " //+ req.Content + ", I am " + *nodeID
	log.Println(text)

	return &proto.SayResp{Content: text}, nil
}

func main() {
	var nodeid = flag.String("snode", "node-1", "NODE ID")
	var port = flag.Int("port", 11111, "PORT")
	var etcd_addr = flag.String("endpoints", "http://127.0.0.1:2379;http://127.0.0.1:2379", "ENDPOINT")
	flag.Parse()

	//check param

	etcd_list := strings.Split(*etcd_addr, ";")

	etcdConfg := etcd3.Config{
		Endpoints: etcd_list,
	}

	zlogger, _ := utils.ZapLoggerInit(SERVICE_NAME)

	//初始化服务注册
	srv_register := srvdiscovery.RegisterConfig{
		EtcdConfig:     etcdConfg,
		Logger:         zlogger,
		RootName:       srvdiscovery.G_ROOT_NAME,
		ServiceType:    enums.ServiceType_RPC,
		ServiceName:    SERVICE_NAME,
		ServiceVersion: "v20190820",
		ServiceNodeID:  *nodeid,
		//RandomSuffix
		NodeData: srvdiscovery.EtcdNodeJsonData{
			AddrInfo: fmt.Sprintf("127.0.0.1:%d", *port),
			Metadata: map[string]string{"weight": "1"},
		},
		Ttl: 10 * time.Second,
	}

	registrar, err := srvdiscovery.NewEtcdV3Register(srv_register)
	if err != nil {
		log.Panic(err)
		return
	}
	server := NewRpcServer(fmt.Sprintf("0.0.0.0:%d", *port))
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		server.Run()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		registrar.RegisterByTimer()
		//registrar.RegisterByApi()
		wg.Done()
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	registrar.UnRegister()
	server.GracefulStop()
	wg.Wait()

}
