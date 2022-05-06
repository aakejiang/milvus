package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	CreatePartition(ctx context.Context, coll *model.Collection, partitionInfo *model.Partition, ts typeutil.Timestamp) error
	CreateIndex(ctx context.Context, index *model.SegmentIndex) error
	CreateAlias(ctx context.Context, collAlias *model.CollectionAlias, ts typeutil.Timestamp) error
}
