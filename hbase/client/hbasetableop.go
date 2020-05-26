package client

import (
	"github.com/liyuan35023/log/hbase/conf"
	"github.com/liyuan35023/log/hbase/model"

	"github.com/tsuna/gohbase"
)

// HBaseOp supports Get, Put, Scan, Delete operations.
type HBaseOp interface {
}

type SimpleHBaseTableOp struct {
	Name        string
	Table       string
	hbaseClient gohbase.Client
}

func NewSimpleHBaseTableOpWithConf(table string, conf *conf.HBaseConf) *SimpleHBaseTableOp {
	return NewSimpleHBaseTableOpWithName(table, conf.ZkRoot, conf.Name)
}

func NewSimpleHBaseTableOpWithName(table string, zks string, name string) *SimpleHBaseTableOp {
	client := hbaseMgr.GetClient(zks)
	return &SimpleHBaseTableOp{
		hbaseClient: client,
		Table:       table,
		Name:        name,
	}
}

func NewSimpleHBaseTableOp(table string, zks string) *SimpleHBaseTableOp {
	return NewSimpleHBaseTableOpWithName(table, zks, "")
}

func (op *SimpleHBaseTableOp) Query(get *model.Get) (*model.HBaseRow, error) {
	getReq, err := model.GenerateHBaseGet(op.Table, get)
	if err != nil {
		return nil, err
	}

	result, err := op.hbaseClient.Get(getReq)
	if err != nil {
		return nil, err
	}

	return model.GenerateHBaseRow(result), nil
}

func (op *SimpleHBaseTableOp) Insert(put *model.Put) error {
	putReq, err := model.GenerateHBasePut(op.Table, put)
	if err != nil {
		return err
	}

	_, err = op.hbaseClient.Put(putReq)
	return err
}

func (op *SimpleHBaseTableOp) Remove(del *model.Delete) error {
	delReq, err := model.GenerateHBaseDelete(op.Table, del)
	if err != nil {
		return err
	}

	_, err = op.hbaseClient.Delete(delReq)

	return err
}

func (op *SimpleHBaseTableOp) GetScanner(scan *model.Scan) (*model.Scanner, error) {
	scanReq, err := model.GenerateHBaseScan(op.Table, scan)
	if err != nil {
		return nil, err
	}
	scanner := op.hbaseClient.Scan(scanReq)
	return model.NewScanner(scanner), nil
}

func (op *SimpleHBaseTableOp) Close() {
	op.hbaseClient.Close()
}

func (op *SimpleHBaseTableOp) GetName() string {
	return op.Name
}
