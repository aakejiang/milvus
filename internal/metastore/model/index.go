package model

import "github.com/milvus-io/milvus/internal/proto/commonpb"

type Index struct {
	CollectionID int64
	Segments     []SegmentIndex
	FieldID      int64
	IndexID      int64
	IndexName    string
	IndexParams  []*commonpb.KeyValuePair
	Extra        map[string]string
}

type SegmentIndex struct {
	Segment        Segment
	EnableIndex    bool
	BuildID        int64
	IndexSize      uint64
	IndexFilePaths []string
}
