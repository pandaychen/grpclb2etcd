package balancer

import (
	"context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"math/rand"
	"strconv"
	"sync"
	"fmt"
)

const RoundRobin = "roundrobin"

//可以封装balancer.SubConn，对每个Conn增加额外属性，如权重，成功率，服务端负载等

//Picker inited by roundRobinPickerBuilder
type roundRobinPicker struct {
		subConns []balancer.SubConn			//一个balancer.SubConn标识一个长连接
											//subConns 标识所有活动连接数组
        mu       sync.Mutex
        next     int
}


// newRoundRobinBuilder creates a new roundrobin balancer builder.
func newRoundRobinBuilder() balancer.Builder {
	return base.NewBalancerBuilderWithConfig(RoundRobin, &roundRobinPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newRoundRobinBuilder())
}

type roundRobinPickerBuilder struct{}

func (*roundRobinPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("roundrobinPicker: newPicker called with readySCs: %v", readySCs)
	
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	fmt.Println(readySCs)
	var scs []balancer.SubConn
	for addr, sc := range readySCs {
		weight := 1
		m, ok := addr.Metadata.(*map[string]string)
		w, ok := (*m)["weight"]
		if ok {
			n, err := strconv.Atoi(w)
			if err == nil && n > 0 {
				weight = n
			}
		}
		for i := 0; i < weight; i++ {
			scs = append(scs, sc)
		}
	}
	//Build的作用是：根据readyScs，构造LB算法选择用的初始化集合，当然可以根据权重对subConns进行调整
	return &roundRobinPicker{
		subConns: scs,
		next:     rand.Intn(len(scs)),
	}
}


//Picker方法：每次客户端RPC-CALL都会调用
func (p *roundRobinPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	p.mu.Lock()
	sc := p.subConns[p.next]
	p.next = (p.next + 1) % len(p.subConns)
	fmt.Println("picker",p.next)
	p.mu.Unlock()
	return sc, nil, nil
}
