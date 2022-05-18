package table

import (
	"time"
)

type SegmentIndex struct {
	Id int64 `db:"id"`
	// SegmentIndexInfo (CollectionID & PartitionID & SegmentID & FieldID & IndexID & BuildID & EnableIndex)
	CollectionID int64 `db:"collection_id"`
	PartitionID  int64 `db:"partition_id"`
	SegmentID    int64 `db:"segment_id"`
	// FieldIndexInfo (FieldID & IndexID)
	FieldID int64 `db:"field_id"`
	// IndexInfo (IndexID & IndexName & IndexParams)
	IndexID        int64     `db:"index_id"`
	BuildID        int64     `db:"build_id"`
	EnableIndex    bool      `db:"enable_index"`
	IndexFilePaths string    `db:"index_file_paths"`
	IndexSize      uint64    `db:"index_size"`
	IsDeleted      bool      `db:"is_deleted"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}
