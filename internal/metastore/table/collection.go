package table

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Collection struct {
	Id              int64              `db:"id"`
	TenantID        string             `db:"tenant_id"`
	CollectionID    int64              `db:"collection_id"`
	CollectionName  string             `db:"collection_name"`
	CollectionAlias string             `db:"collection_alias"`
	Description     string             `db:"description"`
	AutoID          bool               `db:"auto_id"`
	Timestamp       typeutil.Timestamp `db:"ts"`
	Properties      string             `db:"properties"`
	IsDeleted       bool               `db:"is_deleted"`
	CreatedAt       typeutil.Timestamp `db:"created_at"`
	UpdatedAt       typeutil.Timestamp `db:"updated_at"`
}

type CollProperties struct {
	VirtualChannelNames  []string                  `json:"virtual_channel_names,omitempty"`
	PhysicalChannelNames []string                  `json:"physical_channel_names,omitempty"`
	ShardsNum            int32                     `json:"shards_num,omitempty"`
	StartPositions       []*commonpb.KeyDataPair   `json:"start_positions,omitempty"`
	ConsistencyLevel     commonpb.ConsistencyLevel `json:"consistency_level,omitempty"`
}
