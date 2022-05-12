package table

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type IndexBuilder struct {
	Id int64 `db:"id"`
	// SegmentIndexInfo (CollectionID & PartitionID & SegmentID & FieldID & IndexID & BuildID & EnableIndex)
	CollectionID int64 `db:"collection_id"`
	PartitionID  int64 `db:"partition_id"`
	SegmentID    int64 `db:"segment_id"`
	// FieldIndexInfo (FieldID & IndexID)
	FieldID int64 `db:"field_id"`
	// IndexInfo (IndexID & IndexName & IndexParams)
	IndexID     int64                    `db:"index_id"`
	IndexName   string                   `db:"index_name"`
	IndexParams []*commonpb.KeyValuePair `db:"index_params"`
	//EnableIndex    bool                     `db:"enable_index"`
	BuildID        int64              `db:"build_id"`
	IndexFilePaths []string           `db:"index_file_paths"`
	IndexSize      uint64             `db:"index_size"`
	IsDeleted      bool               `db:"is_deleted"`
	CreatedAt      typeutil.Timestamp `db:"created_at"`
	UpdatedAt      typeutil.Timestamp `db:"updated_at"`
}
