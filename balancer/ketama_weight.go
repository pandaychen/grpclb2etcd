package balancer

//a consistent-hash grpc-balancer

import (
	"../utils"
	"context"
	"fmt"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"strconv"
	"sync"
)

const WeightKetamaName = "ketama_hash"

var DefaultWeightKetamaKey = "ketama-default-hash-key"

func init() {
	fmt.Println("register ketama balancer...")
	balancer.Register(newWeightKetamaHashBuilder(DefaultWeightKetamaKey))
}

func InitWeightKetamaHashBuilder(tt_banlancer_key string) {
	balancer.Register(newWeightKetamaHashBuilder(tt_banlancer_key))
}

func newWeightKetamaHashBuilder(tt_banlancer_key string) balancer.Builder {
	return base.NewBalancerBuilderWithConfig(
		WeightKetamaName, //LOCAL NAME
		&WeightKetamaHashPickerBuilder{
			WeightKetamaHashKey: tt_banlancer_key,
		},
		base.Config{
			HealthCheck: true,
		})
}

type WeightKetamaHashPickerBuilder struct {
	WeightKetamaHashKey string
}

func (b *WeightKetamaHashPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("WeightKetamaHashPicker: newPicker called with readySCs: %v", readySCs)
	fmt.Println(readySCs)
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	picker := &WeightKetamaHashPicker{
		subConns:            make(map[string]balancer.SubConn),
		hash:                utils.NewKetama(10, nil),
		WeightKetamaHashKey: b.WeightKetamaHashKey,
	}

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
			node := wrapAddr(addr.Addr, i)
			picker.hash.AddSrvNode(node)
			picker.subConns[node] = sc
		}
	}
	return picker
}

type WeightKetamaHashPicker struct {
	subConns            map[string]balancer.SubConn
	hash                *utils.KetamaConsistent
	WeightKetamaHashKey string
	mu                  sync.Mutex
}

func (p *WeightKetamaHashPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	var sc balancer.SubConn
	p.mu.Lock()
	//key:= p.WeightKetamaHashKey
	//ok:=true
	key, ok := ctx.Value(p.WeightKetamaHashKey).(string)
	fmt.Println(key, ok, p.WeightKetamaHashKey)
	if ok {
		targetAddr, ok := p.hash.GetSrvNode(key)
		if ok {
			sc = p.subConns[targetAddr]
		}
	}
	p.mu.Unlock()
	return sc, nil, nil
}

func wrapAddr(addr string, idx int) string {
	return fmt.Sprintf("%s-%d", addr, idx)
}
