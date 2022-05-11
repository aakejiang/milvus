package table

import "github.com/milvus-io/milvus/internal/util/typeutil"

type CollectionPartitionR struct {
	CollectionID int64              `db:"collection_id"`
	PartitionID  int64              `db:"partition_id"`
	Timestamp    typeutil.Timestamp `db:"ts"`
}
