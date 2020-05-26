package client

import (
	"fmt"
	"hbase-sdk/model"
	"io"
	"os"
	"testing"
	"time"
)

var hbaseOp *SimpleHBaseTableOp

func setup() {
	hbaseOp = NewSimpleHBaseTableOp(testTable, testZks)
}

func teardown() {
	hbaseOp.Close()
}

func TestMain(m *testing.M) {
	fmt.Println("-----> start testing. <------")
	setup()
	ret := m.Run()
	teardown()
	fmt.Println("-----> end testing. <------")

	os.Exit(ret)
}

func TestSimpleHBaseQuery(t *testing.T) {
	get := model.NewSimpleGet(testRow1)
	rs, err := hbaseOp.Query(get)
	if err != nil {
		t.Error("err happen.", err.Error())
		return
	}

	t.Log("test single query success.")

	t.Log(printPrettyRow(rs))
}

func TestSimpleHBaseVersionsQuery(t *testing.T) {
	get := model.NewVersionsGet(testRow1, 10)
	rs, err := hbaseOp.Query(get)
	if err != nil {
		t.Error("err happen.", err.Error())
		return
	}

	t.Log("test single query versions success.")

	t.Log(printPrettyRow(rs))
}

func printPrettyRow(rs *model.HBaseRow) string {
	if rs == nil || rs.Cells == nil {
		return "Row: nil."
	}

	res := "Row: \n"
	for _, cell := range rs.Cells {
		res += fmt.Sprintf("Row: rowkey [%s], timestamp [%s], family [%s], qualifier [%s], value [%s] \n",
			cell.RowKey, time.Unix(int64(cell.Timestamp/1000), int64(cell.Timestamp%1000)).Format("2006-01-02 15:04:05"),
			cell.Family, cell.Qualifier, cell.Value)
	}

	return res
}

func TestSimpleHBasePut(t *testing.T) {
	put := model.NewPut(testRow1, generateTestVals())
	err := hbaseOp.Insert(put)
	if err != nil {
		t.Error("err happen.", err.Error())
		return
	}

	t.Log("test single put success.")
}

func TestSimpleHBasePutWithTimestamp(t *testing.T) {
	put := model.NewPutWithTimestamp(testRow1, generateTestVals(), time.Now().UnixNano()/(1000*1000)-int64(3600*1000))
	err := hbaseOp.Insert(put)
	if err != nil {
		t.Error("err happen.", err.Error())
		return
	}

	t.Log("test single put with timestamp success.")
}

func TestSimpleHBaseDelete(t *testing.T) {
	del := model.NewDelete(testRow1)
	err := hbaseOp.Remove(del)
	if err != nil {
		t.Error("err happen.", err.Error())
		return
	}

	t.Log("test single delete success.")
}

func TestSimpleHBasePrefixScan(t *testing.T) {
	for _, row := range testScanRow {
		hbaseOp.Insert(model.NewPut(row, generateTestVals()))
	}

	scan := model.NewScanPrefix("some")
	scanner, err := hbaseOp.GetScanner(scan)
	if err != nil {
		t.Error("err happen.", err.Error())
		return
	}

	defer scanner.Close()
	rs, err := scanner.Next()
	for ; rs != nil && err == nil; rs, err = scanner.Next() {
		t.Log(printPrettyRow(rs))
	}

	if err != nil && err != io.EOF {
		t.Error("err happen.", err.Error())
	}

	t.Log("test scan prefix success.")
}
