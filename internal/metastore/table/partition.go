package table

import "github.com/milvus-io/milvus/internal/util/typeutil"

type Partition struct {
	Id                        int64              `db:"id"`
	PartitionID               int64              `db:"partition_id"`
	PartitionName             string             `db:"partition_name"`
	PartitionCreatedTimestamp uint64             `db:"partition_created_timestamp"`
	CollectionID              int64              `db:"collection_id"`
	Timestamp                 typeutil.Timestamp `db:"ts"`
	IsDeleted                 bool               `db:"is_deleted"`
	CreatedAt                 typeutil.Timestamp `db:"created_at"`
	UpdatedAt                 typeutil.Timestamp `db:"updated_at"`
}
