package client

import (
	"github.com/liyuan35023/log/hbase/model"
	"testing"
)

func TestCreateTable(t *testing.T) {
	hbaseAdmin := NewHBaseAdminClient(testZks)
	tableDesc := model.NewTableDescriptor(testTable, testTableCf1, testTableCf2)

	err := hbaseAdmin.CreateTable(tableDesc)
	if err != nil {
		t.Error(err.Error())
		return
	}

	t.Logf("create table [%s] success.", testTable)
}

func TestDisableTable(t *testing.T) {
	hbaseAdmin := NewHBaseAdminClient(testZks)
	err := hbaseAdmin.DisableTable(testTable)
	if err != nil {
		t.Error(err.Error())
		return
	}

	t.Logf("disable table [%s] success.", testTable)
}

func TestEnableTable(t *testing.T) {
	hbaseAdmin := NewHBaseAdminClient(testZks)
	err := hbaseAdmin.EnableTable(testTable)
	if err != nil {
		t.Error(err.Error())
		return
	}

	t.Logf("enable table [%s] success.", testTable)
}

func TestDeleteTable(t *testing.T) {
	hbaseAdmin := NewHBaseAdminClient(testZks)

	err := hbaseAdmin.DeleteTable(testTable)
	if err != nil {
		t.Error(err.Error())
		return
	}

	t.Logf("delete table [%s] success.", testTable)
}
