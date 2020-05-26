package model

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/tsuna/gohbase/hrpc"
)

const (
	MUTATE_PUT = iota
	MUTATE_DELETE
)

type Mutate struct {
	Wal       bool
	Rowkey    string
	Timestamp int64
	KeyValues map[string]map[string][]byte
}

func generateHBaseMutate(table string, mutate *Mutate, mutateType int) (*hrpc.Mutate, error) {
	if mutate == nil {
		return nil, nil
	}
	options := []func(hrpc.Call) error{}
	if !mutate.Wal {
		durationOption := hrpc.Durability(hrpc.SkipWal)
		options = append(options, durationOption)
	}
	if mutate.Timestamp > 0 {
		ts := time.Unix(0, mutate.Timestamp*int64(time.Millisecond))
		timestampOption := hrpc.Timestamp(ts)
		options = append(options, timestampOption)
	}

	// skip batch true to avoid data lose, may decrease effective
	options = append(options, hrpc.SkipBatch())

	var rsMutate *hrpc.Mutate
	var err error
	switch mutateType {
	case MUTATE_PUT:
		rsMutate, err = hrpc.NewPutStr(context.Background(), table, mutate.Rowkey, mutate.KeyValues, options...)
	case MUTATE_DELETE:
		rsMutate, err = hrpc.NewDelStr(context.Background(), table, mutate.Rowkey, mutate.KeyValues, options...)
	default:
		err = errors.New("unsupported type." + strconv.Itoa(mutateType))
	}
	if err != nil {
		return nil, err
	}
	return rsMutate, nil
}

type Put Mutate

func NewPut(rowkey string, kvs map[string]map[string][]byte) *Put {
	return NewPutWithTimestamp(rowkey, kvs, 0)
}

// timestamp use millisecond
func NewPutWithTimestamp(rowkey string, kvs map[string]map[string][]byte, timestamp int64) *Put {
	return &Put{
		Wal:       true,
		Rowkey:    rowkey,
		Timestamp: timestamp,
		KeyValues: kvs,
	}
}

// GenerateHBasePut transfer model.Put to hrpc.Put
func GenerateHBasePut(table string, put *Put) (*hrpc.Mutate, error) {
	return generateHBaseMutate(table, (*Mutate)(put), MUTATE_PUT)
}

type Delete Mutate

func NewDelete(rowkey string) *Delete {
	return NewDeleteWithTimestamp(rowkey, 0)
}

func NewDeleteWithTimestamp(rowkey string, timestamp int64) *Delete {
	return NewSpecDeleteWithTimestamp(rowkey, nil, timestamp)
}

func NewSpecDelete(rowkey string, kvs map[string]map[string][]byte) *Delete {
	return NewSpecDeleteWithTimestamp(rowkey, kvs, 0)
}

func NewSpecDeleteWithTimestamp(rowkey string, kvs map[string]map[string][]byte, timestamp int64) *Delete {
	return &Delete{
		Wal:       true,
		Rowkey:    rowkey,
		Timestamp: timestamp,
		KeyValues: kvs,
	}
}

func GenerateHBaseDelete(table string, delete *Delete) (*hrpc.Mutate, error) {
	return generateHBaseMutate(table, (*Mutate)(delete), MUTATE_DELETE)
}
