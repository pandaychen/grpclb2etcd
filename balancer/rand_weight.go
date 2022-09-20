package balancer

import (
	mrand "math/rand"
	"strconv"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const RandomWeight = "RandomWeight"

func newRandomBuilder() balancer.Builder {
	return base.NewBalancerBuilder(RandomWeight, &randomPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	//register randombuild to balancer
	balancer.Register(newRandomBuilder())
}

type randomPickerBuilder struct{}

func (*randomPickerBuilder) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	if len(buildInfo.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	var (
		scs []balancer.SubConn
	)

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
			scs = append(scs, subconn)
		}
	}

	return &randomPicker{
		subConns: scs,
	}
}

type randomPicker struct {
	seed     int64
	subConns []balancer.SubConn
	mu       sync.Mutex
}

//Pick One available connection
func (p *randomPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	var (
		pickResult balancer.PickResult
	)
	p.mu.Lock()
	index := mrand.Intn(len(p.subConns))
	//fmt.Println(index,p.subConns)
	sc := p.subConns[index]
	p.mu.Unlock()

	pickResult.SubConn = sc.(balancer.SubConn)
	return pickResult, nil
}
