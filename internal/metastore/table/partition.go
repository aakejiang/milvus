package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Partition struct {
	Id                        *int64             `db:"id"`
	PartitionID               *int64             `db:"partition_id"`
	PartitionName             *string            `db:"partition_name"`
	PartitionCreatedTimestamp *uint64            `db:"partition_created_timestamp"`
	CollectionID              *int64             `db:"collection_id"`
	Ts                        typeutil.Timestamp `db:"ts"`
	IsDeleted                 *bool              `db:"is_deleted"`
	CreatedAt                 *time.Time         `db:"created_at"`
	UpdatedAt                 *time.Time         `db:"updated_at"`
}
