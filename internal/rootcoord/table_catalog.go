package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/db"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
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

func (tc *TableCatalog) CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *TableCatalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	return nil
}

func (tc *TableCatalog) GetCollection(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*datapb.CollectionInfo, error) {
	return nil, nil
}

func (tc *TableCatalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	return false
}

func (tc *TableCatalog) AlterAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func (tc *TableCatalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*etcdpb.CollectionInfo, error) {
	return nil, nil
}
