package client

import (
	"strings"
	"time"
)

var (
	testZks = "10.16.54.100:2181,10.16.54.101:2181,10.16.54.102:2181,10.16.54.103:2181,10.16.54.104:2181/hbase"

	testTable    = "sdk_client_test"
	testTableCf1 = "sdk_client_test_cf1"
	testTableCf2 = "sdk_client_test_cf2"
	testCol1     = "sdk_col1"
	testCol2     = "sdk_col2"

	testRow1 = "test0001"

	testScanRow = []string{"scantest0001", "scantest0002", "scantest0003", "sometest0001", "othertest0002", "someothertest0003"}
)

func generateTestVals() map[string]map[string][]byte {
	timeStr := time.Now().Format("2006-01-02 15:04:05")

	return map[string]map[string][]byte{
		testTableCf1: map[string][]byte{
			testCol1: []byte(strings.Join([]string{testTableCf1, testCol1, timeStr}, "|")),
			testCol2: []byte(strings.Join([]string{testTableCf1, testCol2, timeStr}, "|")),
		},
		testTableCf2: map[string][]byte{
			testCol1: []byte(strings.Join([]string{testTableCf2, testCol1, timeStr}, "|")),
			testCol2: []byte(strings.Join([]string{testTableCf2, testCol2, timeStr}, "|")),
		},
	}
}
