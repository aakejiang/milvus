package metastore

import (
	"context"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog interface {
	CreateCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error
	GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error)
	GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error)
	ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error)
	CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool
	DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error

	CreatePartition(ctx context.Context, coll *model.Collection, partition *model.Partition, ts typeutil.Timestamp) error
	DropPartition(ctx context.Context, collectionInfo *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error

	CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error
	AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index) error
	DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID) error
	ListIndexes(ctx context.Context) ([]*model.Index, error)

	AddAlias(ctx context.Context, collAlias *model.CollectionAlias, ts typeutil.Timestamp) error
	DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error
	ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Collection, error)

	GetCredential(ctx context.Context, username string) (*model.Credential, error)
	CreateCredential(ctx context.Context, credential *model.Credential) error
	DropCredential(ctx context.Context, username string) error
	ListCredentials(ctx context.Context) ([]string, error)

	IsDDMsgSent(ctx context.Context) (bool, error)
	LoadDdOperation(ctx context.Context) (model.DdOperation, error)
	UpdateDDOperation(ctx context.Context, ddOp model.DdOperation, isSent bool) error

	Close()
}
