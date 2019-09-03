package balancer

import (
	"context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
	mrand "math/rand"
	"strconv"
	"sync"
)

const RandomWeight = "RandomWeight"

func newRandomBuilder() balancer.Builder {
	return base.NewBalancerBuilderWithConfig(RandomWeight, &randomPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	//register randombuild to balancer
	balancer.Register(newRandomBuilder())
}

type randomPickerBuilder struct{}

func (*randomPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
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

	return &randomPicker{
		subConns: scs,
	}
}

type randomPicker struct {
	seed int64
	subConns []balancer.SubConn
	mu   sync.Mutex
}

//Pick One available connection
func (p *randomPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	p.mu.Lock()
	index:=mrand.Intn(len(p.subConns))
	//fmt.Println(index,p.subConns)
	sc := p.subConns[index]
	p.mu.Unlock()
	return sc, nil, nil
}
