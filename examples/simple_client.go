package main

//A SIMPLE HELLO-WORLD CLIENT
//author:pandaychen

import (
	"flag"
	"grpclb2etcd/balancer"
	"log"
	"strings"
	"time"

	"grpclb2etcd/utils"

	proto "grpclb2etcd/proto"

	srvdiscovery "grpclb2etcd/srv_discovery"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	var etcd_addr = flag.String("endpoints", "http://127.0.0.1:2379;http://127.0.0.1:2379", "ENDPOINT")
	flag.Parse()

	etcdlist := strings.Split(*etcd_addr, ";")

	etcdConfg := etcdv3.Config{
		Endpoints: etcdlist,
	}

	etcdpath := srvdiscovery.EtcdKeyDirPath{
		RootName: srvdiscovery.G_ROOT_NAME,
		//ServiceType:    enums.ServiceType_RPC,
		ServiceName:    "helloworld",
		ServiceVersion: "v20190820"}

	zlogger, _ := utils.ZapLoggerInit("helloworld")
	//fmt.Println(etcdpath)
	srvdiscovery.RegisterResolver("etcdv3", etcdConfg, etcdpath, zlogger)

	//Dial-"etcd3:///" 指定reslver WithBalancerName--指定balancer
	c, err := grpc.Dial("etcdv3:///", grpc.WithInsecure(), grpc.WithBalancerName(balancer.RoundRobin))
	if err != nil {
		log.Printf("grpc dial error: %s", err)
		return
	}
	defer c.Close()
	client := proto.NewTestClient(c)
	for i := 0; i < 5000000; i++ {
		resp, err := client.Say(context.Background(), &proto.SayReq{Content: "round robin"})
		if err != nil {
			log.Println("get error:", err)
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
		log.Printf(resp.Content)
	}
}
