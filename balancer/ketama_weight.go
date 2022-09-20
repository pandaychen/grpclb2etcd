package balancer

//a consistent-hash grpc-balancer

import (
	"fmt"
	"strconv"
	"sync"

	"grpclb2etcd/utils"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const WeightKetamaName = "ketama_hash"

var DefaultWeightKetamaKey = "ketama-default-hash-key"

func init() {
	balancer.Register(newWeightKetamaHashBuilder(DefaultWeightKetamaKey))
}

func InitWeightKetamaHashBuilder(tt_banlancer_key string) {
	balancer.Register(newWeightKetamaHashBuilder(tt_banlancer_key))
}

func newWeightKetamaHashBuilder(tt_banlancer_key string) balancer.Builder {
	return base.NewBalancerBuilder(
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

func (b *WeightKetamaHashPickerBuilder) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	if len(buildInfo.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	picker := &WeightKetamaHashPicker{
		subConns:            make(map[string]balancer.SubConn),
		hash:                utils.NewKetama(10, nil),
		WeightKetamaHashKey: b.WeightKetamaHashKey,
	}

	for subconn, sc := range buildInfo.ReadySCs {
		weight := 1
		m, ok := sc.Address.Metadata.(*map[string]string)
		w, ok := (*m)["weight"]
		if ok {
			n, err := strconv.Atoi(w)
			if err == nil && n > 0 {
				weight = n
			}
		}
		for i := 0; i < weight; i++ {
			node := wrapAddr(sc.Address.Addr, i)
			picker.hash.AddSrvNode(node)
			picker.subConns[node] = subconn
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

func (p *WeightKetamaHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var (
		pickResult balancer.PickResult
		sc         balancer.SubConn
	)
	p.mu.Lock()
	//key:= p.WeightKetamaHashKey
	//ok:=true
	//key, ok := ctx.Value(p.WeightKetamaHashKey).(string)
	key, ok := info.Ctx.Value(p.WeightKetamaHashKey).(string)
	//fmt.Println(key, ok, p.WeightKetamaHashKey)
	if ok {
		targetAddr, ok := p.hash.GetSrvNode(key)
		if ok {
			sc = p.subConns[targetAddr]
		}
	}
	p.mu.Unlock()

	pickResult.SubConn = sc.(balancer.SubConn)
	return pickResult, nil
}

func wrapAddr(addr string, idx int) string {
	return fmt.Sprintf("%s-%d", addr, idx)
}
