package model

import (
	"context"
	"errors"

	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
)

type Scan struct {
	StartRow string
	StopRow  string
	Prefix   string
	Batch    uint32
	Filters  filter.Filter
}

const (
	defaultStartRow = ""
	defaultEndRow   = ""
	defaultPrefix   = ""
	defaultCache    = 64
)

func NewScan(start string, stop string, prefix string, batch uint32, filter filter.Filter) *Scan {
	return &Scan{
		StartRow: start,
		StopRow:  stop,
		Prefix:   prefix,
		Batch:    batch,
		Filters:  filter,
	}
}

func NewScanRange(start string, stop string) *Scan {
	return NewScan(start, stop, defaultPrefix, defaultCache, nil)
}

func NewScanPrefix(prefix string) *Scan {
	return NewScan(defaultStartRow, defaultEndRow, prefix, defaultCache, nil)
}

func NewScanRangeWithFilter(start string, stop string, filter *filter.FamilyFilter) *Scan {
	return NewScan(start, stop, defaultPrefix, defaultCache, filter)
}

func GenerateHBaseScan(table string, scan *Scan) (*hrpc.Scan, error) {
	if scan == nil {
		return nil, nil
	}
	options := []func(hrpc.Call) error{}
	if scan.Batch > 0 {
		options = append(options, hrpc.NumberOfRows(scan.Batch))
	}
	filters := []filter.Filter{}
	if scan.Filters != nil {
		filters = append(filters, scan.Filters)
	}
	if scan.Prefix != "" {
		prefixFilter := filter.NewPrefixFilter([]byte(scan.Prefix))
		filters = append(filters, prefixFilter)
	}
	var scanFilter filter.Filter
	if len(filters) > 1 {
		scanFilter = filter.NewList(filter.MustPassAll, filters...)
	} else if len(filters) == 1 {
		scanFilter = filters[0]
	} else {
		scanFilter = nil
	}
	if scanFilter != nil {
		options = append(options, hrpc.Filters(scanFilter))
	}

	hbaseScan, err := hrpc.NewScanRangeStr(context.Background(), table, scan.StartRow, scan.StopRow, options...)
	if err != nil {
		return nil, err
	}

	return hbaseScan, nil
}

type Scanner struct {
	scanner hrpc.Scanner
}

func NewScanner(scanner hrpc.Scanner) *Scanner {
	return &Scanner{scanner: scanner}
}

func (s *Scanner) Next() (*HBaseRow, error) {
	if s.scanner == nil {
		return nil, errors.New("nil scanner error.")
	}

	res, err := s.scanner.Next()
	var row *HBaseRow
	if res != nil {
		row = GenerateHBaseRow(res)
	}

	return row, err
}

func (s *Scanner) Close() error {
	if s.scanner != nil {
		return s.scanner.Close()
	}

	return nil
}
