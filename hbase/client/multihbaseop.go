package client

import (
	"context"
	"errors"
	"github.com/liyuan35023/utils/hbase/conf"
	"github.com/liyuan35023/utils/hbase/model"

	"time"
)

// MultiHBaseOp use numbers of HBase to ensure avalibility.
type MultiHBaseOp struct {
	tableopList  []TableOp
	queryMaster  TableOp
	mutateMaster TableOp
	scanMaster   TableOp
	kafkaClient  *KafkaClient
	config       *conf.MultiHBaseConf
}

// NewMultiHBaseOp create MutliHBaseOp.
func NewMultiHBaseOp(tablename string, config *conf.MultiHBaseConf, kafkaconf *conf.KafkaConf) *MultiHBaseOp {
	// todo init
	resOp := new(MultiHBaseOp)
	tableOpList := []TableOp{}
	for name, tableOpConf := range config.HBaseConfigs {
		tableOp := NewSimpleHBaseTableOpWithConf(tablename, tableOpConf)
		tableOpList = append(tableOpList, tableOp)
		if name == config.QueryMaster {
			resOp.queryMaster = tableOp
		}
		if name == config.MutateMaster {
			resOp.mutateMaster = tableOp
		}
		if name == config.ScanMaster {
			resOp.scanMaster = tableOp
		}
	}
	resOp.tableopList = tableOpList
	resOp.config = config

	return resOp
}

// Query operation in MultiHBaseOp query first in master hbase, after 30ms, query in other hbase
func (op *MultiHBaseOp) Query(get *model.Get) (*model.HBaseRow, error) {
	resCh := make(chan *model.HBaseRow)
	defer close(resCh)

	count := len(op.tableopList)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// query master hbase
	go queryRoutine(ctx, get, op.queryMaster, resCh)

	var res *model.HBaseRow

	// 30 ms timeout
	select {
	case res = <-resCh:
		count--
	case <-time.After(time.Duration(op.config.QueryMasterTimeout) * time.Millisecond):
	}

	if res != nil && len(res.Cells) != 0 {
		return res, nil
	}

	for _, otherOp := range op.tableopList {
		if otherOp != op.queryMaster {
			go queryRoutine(ctx, get, otherOp, resCh)
		}
	}

	if count == 0 {
		return res, nil
	}

	for res = range resCh {
		count--
		if res != nil && len(res.Cells) != 0 {
			return res, nil
		}

		// all tableop queried, and no result.
		if count == 0 {
			break
		}
	}

	return nil, nil
}

// todo: add hystrix command
func queryRoutine(ctx context.Context, get *model.Get, singleOp TableOp, resCh chan<- *model.HBaseRow) {
	resRow, err := singleOp.Query(get)
	if err != nil {
		// todo: error log
		resRow = nil
	}

	// accept cancel
	select {
	case <-ctx.Done():
	case resCh <- resRow:
	}
}

func (op *MultiHBaseOp) Insert(put *model.Put) error {
	resCh := make(chan *InsertResult)
	defer close(resCh)
	count := len(op.tableopList)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// insert master
	go insertRoutine(ctx, put, op.mutateMaster, resCh)
	success := false
	successName := ""

	var err error
	select {
	case insertRes := <-resCh:
		count--
		err = insertRes.err
		// success
		if insertRes.err == nil {
			success = true
			successName = insertRes.name
		}
	case <-time.After(time.Duration(op.config.MutateMasterTimeout) * time.Millisecond):
	}

	// master not success. send put request to other hbase.
	if !success && count > 0 {
		for _, otherOp := range op.tableopList {
			if otherOp != op.mutateMaster {
				go insertRoutine(ctx, put, otherOp, resCh)
			}
		}

		for insertRes := range resCh {
			count--
			// success
			if insertRes.err == nil {
				success = true
				successName = insertRes.name
				break
			} else {
				err = insertRes.err
			}
			if count == 0 {
				err = errors.New("[HBaseErr] All HBase put failed")
				break
			}
		}
	}

	if success {
		// async write to kafka
		op.kafkaClient.Insert(put, successName)

		return nil
	}

	return err
}

type InsertResult struct {
	err  error
	name string
}

func insertRoutine(ctx context.Context, put *model.Put, singleOp TableOp, resCh chan<- *InsertResult) {
	err := singleOp.Insert(put)

	select {
	case <-ctx.Done():
	case resCh <- &InsertResult{err: err, name: singleOp.GetName()}:
	}
}

func (op *MultiHBaseOp) Remove(del *model.Delete) error {
	resCh := make(chan error)
	defer close(resCh)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, singleOp := range op.tableopList {
		go removeRoutine(ctx, del, singleOp, resCh)
	}

	var err error
	for err = range resCh {
		if err != nil {
			return err
		}
	}

	return nil
}

func removeRoutine(ctx context.Context, delete *model.Delete, singleOp TableOp, resCh chan<- error) {
	err := singleOp.Remove(delete)
	select {
	case <-ctx.Done():
	case resCh <- err:
	}
}

func (op *MultiHBaseOp) GetScanner(scan *model.Scan) (*model.Scanner, error) {
	resCh := make(chan *model.Scanner)
	defer close(resCh)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, singleOp := range op.tableopList {
		go scanRoutine(ctx, scan, singleOp, resCh)
	}

	for scanner := range resCh {
		if scanner != nil {
			return scanner, nil
		}
	}

	return nil, nil
}

func scanRoutine(ctx context.Context, scan *model.Scan, singleOp TableOp, resCh chan<- *model.Scanner) {
	scanner, err := singleOp.GetScanner(scan)
	if err != nil {
		// error log
		scanner = nil
	}

	select {
	case <-ctx.Done():
	case resCh <- scanner:
	}
}
