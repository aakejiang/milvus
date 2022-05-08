package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error)
	GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error)

	CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool
	DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error

	CreatePartition(ctx context.Context, coll *model.Collection, partitionInfo *model.Partition, ts typeutil.Timestamp) error
	GetPartition(ctx context.Context, collectionName string, partitionName string) (*model.Partition, error)
	GetPartitionWithVersion(ctx context.Context, collectionName string, partitionName string, version int) (model.Partition, error)
	DropPartition(ctx context.Context, collectionInfo *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error

	CreateIndex(ctx context.Context, index *model.Index) error
	DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error

	GetCredential(ctx context.Context, username string) (*model.Credential, error)
	CreateCredential(ctx context.Context, credential *model.Credential) error
	DropCredential(ctx context.Context, username string) error

	CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error
	DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error
	AlterAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error

	ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error)
	ListIndexes(ctx context.Context) ([]*model.Index, error)
	ListCredentials(ctx context.Context) ([]string, error)

	Close()
}
