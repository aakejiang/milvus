package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/db"
	"github.com/milvus-io/milvus/internal/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type TableCatalog struct {
	db *db.DB
}

func (tc *TableCatalog) CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *TableCatalog) CreatePartition(ctx context.Context, coll *model.Collection, partitionInfo *model.Partition, ts typeutil.Timestamp) error {
	return nil
}

func (tc *TableCatalog) CreateIndex(ctx context.Context, index *model.SegmentIndex) error {
	return nil
}
