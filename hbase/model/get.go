package model

import (
	"context"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
)

const (
	defaultMaxVersion = 1
)

type Get struct {
	Rowkey     string
	Families   map[string][]string
	Filters    filter.Filter
	MaxVersion uint32
}

func NewGet(rowkey string, families map[string][]string, filters filter.Filter, maxVersion uint32) *Get {
	return &Get{
		Rowkey:     rowkey,
		Families:   families,
		Filters:    filters,
		MaxVersion: maxVersion,
	}
}

func NewVersionsGet(rowkey string, maxVersion uint32) *Get {
	return NewGet(rowkey, nil, nil, maxVersion)
}

func NewSimpleGet(rowkey string) *Get {
	return NewGet(rowkey, nil, nil, 1)
}

// GenerateHBaseGet transfer model.Get to hrpc.Get
func GenerateHBaseGet(table string, get *Get) (*hrpc.Get, error) {
	if get == nil {
		return nil, nil
	}
	options := []func(hrpc.Call) error{}
	if get.MaxVersion > 1 {
		maxVersionOption := hrpc.MaxVersions(get.MaxVersion)
		options = append(options, maxVersionOption)
	}
	if get.Families != nil && len(get.Families) > 0 {
		familyOption := hrpc.Families(get.Families)
		options = append(options, familyOption)
	}
	if get.Filters != nil {
		filterOption := hrpc.Filters(get.Filters)
		options = append(options, filterOption)
	}
	getReq, err := hrpc.NewGetStr(context.Background(), table, get.Rowkey, options...)
	if err != nil {
		return nil, err
	}

	return getReq, nil
}
