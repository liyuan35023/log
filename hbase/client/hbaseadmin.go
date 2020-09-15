package client

import (
	"context"
	"github.com/liyuan35023/utils/hbase/model"

	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

// HBaseAdminClient used to execute table related operation
// include create table, delete table, modify table
type HBaseAdminClient struct {
	adminClient gohbase.AdminClient
}

// NewHBaseAdminClient create an hbase admin client, need zk node.
func NewHBaseAdminClient(zks string) *HBaseAdminClient {
	adminClient := gohbase.NewAdminClient(zks)
	if adminClient == nil {
		return nil
	}
	return &HBaseAdminClient{
		adminClient: adminClient,
	}
}

// CreateTable create a table descriped by tableDes.
func (c *HBaseAdminClient) CreateTable(tableDes *model.TableDescriptor) error {
	createTableReq := hrpc.NewCreateTable(context.Background(), []byte(tableDes.GetTable()), tableDes.GetFamilies())
	return c.adminClient.CreateTable(createTableReq)
}

// DeleteTable disable table first, then delete table.
func (c *HBaseAdminClient) DeleteTable(tableName string) error {
	err := c.DisableTable(tableName)
	if err != nil {
		return err
	}

	deleteTableReq := hrpc.NewDeleteTable(context.Background(), []byte(tableName))
	return c.adminClient.DeleteTable(deleteTableReq)
}

// DisableTable disable table, if table is disable, return error.
func (c *HBaseAdminClient) DisableTable(tableName string) error {
	disableTableReq := hrpc.NewDisableTable(context.Background(), []byte(tableName))
	return c.adminClient.DisableTable(disableTableReq)
}

// EnableTable enable table, if table is enable, return error.
func (c *HBaseAdminClient) EnableTable(tableName string) error {
	enableTableReq := hrpc.NewEnableTable(context.Background(), []byte(tableName))
	return c.adminClient.EnableTable(enableTableReq)
}
