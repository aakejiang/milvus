package model

import "github.com/milvus-io/milvus/internal/proto/commonpb"

type Index struct {
	CollectionID   int64
	FieldID        int64
	IndexID        int64
	IndexName      string
	IndexParams    []*commonpb.KeyValuePair
	SegmentIndexes []SegmentIndex
	Extra          map[string]string
}
