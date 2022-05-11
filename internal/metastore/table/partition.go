package table

import "github.com/milvus-io/milvus/internal/util/typeutil"

type Partition struct {
	PartitionID               int64              `db:"id"`
	PartitionName             string             `db:"partition_name"`
	PartitionCreatedTimestamp uint64             `db:"partition_created_timestamp"`
	CollectionID              int64              `db:"collection_id"`
	Timestamp                 typeutil.Timestamp `db:"ts"`
}
