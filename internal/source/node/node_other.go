//go:build !linux

package node

import (
	"errors"
	dto "github.com/prometheus/client_model/go"
)

type Collector struct{}

func New([]string) (*Collector, error) { return nil, errors.New("host collection requires Linux") }
func (*Collector) Gather() ([]*dto.MetricFamily, error) {
	return nil, errors.New("host collection requires Linux")
}
