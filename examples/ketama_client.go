package main

//author:pandaychen

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"grpclb2etcd/balancer"
	"grpclb2etcd/enums"
	proto "grpclb2etcd/proto"
	srvdiscovery "grpclb2etcd/srv_discovery"
	"grpclb2etcd/utils"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

func main() {
	var etcd_addr = flag.String("endpoints", "http://127.0.0.1:2379;http://127.0.0.1:2379", "ENDPOINT")

	etcdlist := strings.Split(*etcd_addr, ";")

	etcdConfg := etcdv3.Config{
		Endpoints: etcdlist,
	}

	etcdpath := srvdiscovery.EtcdKeyDirPath{
		RootName:       srvdiscovery.G_ROOT_NAME,
		ServiceType:    enums.ServiceType_RPC,
		ServiceName:    "helloworld",
		ServiceVersion: "v20190820"}

	zlogger, _ := utils.ZapLoggerInit("helloworld")
	//balancer.InitConsistentHashBuilder(balancer.DefaultConsistentHashKey)
	srvdiscovery.RegisterResolver(string(enums.RT_ETCDV3), etcdConfg, etcdpath, zlogger) //第一个参数

	//Dial-"etcd3:///" 指定reslver WithBalancerName--指定balancer

	grpc_resovler := fmt.Sprintf("%s:///", enums.RT_ETCDV3)

	c, err := grpc.Dial(grpc_resovler, grpc.WithInsecure(), grpc.WithBalancerName(balancer.WeightKetamaName))

	if err != nil {
		log.Printf("grpc dial error: %s", err)
		return
	}
	defer c.Close()
	client := proto.NewTestClient(c)

	for i := 0; i < 5000000; i++ {
		//context.WithValue(ctx, balancer.DefaultConsistentHashKey
		ctx := context.Background()

		hashData := fmt.Sprintf("%d", i)
		//hashData="fix call server addr"
		resp, err := client.Say(context.WithValue(ctx, balancer.DefaultWeightKetamaKey, hashData),
			&proto.SayReq{Content: "ketama"})
		if err != nil {
			log.Println("get error:", err)
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
		log.Printf(resp.Content)
	}
}
