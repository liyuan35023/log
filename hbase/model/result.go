package model

import (
	"github.com/tsuna/gohbase/hrpc"
)

type HBaseRow struct {
	Cells []*HBaseCell
}

type HBaseCell struct {
	RowKey    []byte
	Family    []byte
	Qualifier []byte
	Timestamp uint64
	Value     []byte
}

func GenerateHBaseRow(res *hrpc.Result) *HBaseRow {
	if res == nil {
		return nil
	}

	if res.Cells == nil || len(res.Cells) == 0 {
		return &HBaseRow{}
	}

	length := len(res.Cells)
	cells := make([]*HBaseCell, length, length)
	for idx, val := range res.Cells {
		cells[idx] = GenerateHBaseCell(val)
	}

	return &HBaseRow{
		Cells: cells,
	}
}

func GenerateHBaseCell(cell *hrpc.Cell) *HBaseCell {
	return &HBaseCell{
		RowKey:    cell.Row,
		Family:    cell.Family,
		Qualifier: cell.Qualifier,
		Timestamp: *cell.Timestamp,
		Value:     cell.Value,
	}
}
