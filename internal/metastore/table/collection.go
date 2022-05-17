package table

import (
	"database/sql"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Collection struct {
	Id              int64              `db:"id"`
	TenantID        sql.NullString     `db:"tenant_id"`
	CollectionID    int64              `db:"collection_id"`
	CollectionName  string             `db:"collection_name"`
	CollectionAlias sql.NullString     `db:"collection_alias"`
	Description     sql.NullString     `db:"description"`
	AutoID          bool               `db:"auto_id"`
	Ts              typeutil.Timestamp `db:"ts"`
	Properties      sql.NullString     `db:"properties"`
	IsDeleted       bool               `db:"is_deleted"`
	CreatedAt       time.Time          `db:"created_at"`
	UpdatedAt       time.Time          `db:"updated_at"`
}

type CollProperties struct {
	VirtualChannelNames  []string                  `json:"virtual_channel_names,omitempty"`
	PhysicalChannelNames []string                  `json:"physical_channel_names,omitempty"`
	ShardsNum            int32                     `json:"shards_num,omitempty"`
	StartPositions       []*commonpb.KeyDataPair   `json:"start_positions,omitempty"`
	ConsistencyLevel     commonpb.ConsistencyLevel `json:"consistency_level,omitempty"`
}
