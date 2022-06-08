package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Field struct {
	Id           int64              `db:"id"`
	TenantID     *string            `db:"tenant_id"`
	FieldID      int64              `db:"field_id"`
	FieldName    string             `db:"field_name"`
	IsPrimaryKey bool               `db:"is_primary_key"`
	Description  *string            `db:"description"`
	DataType     schemapb.DataType  `db:"data_type"`
	TypeParams   *string            `db:"type_params"`
	IndexParams  *string            `db:"index_params"`
	AutoID       bool               `db:"auto_id"`
	CollectionID int64              `db:"collection_id"`
	Ts           typeutil.Timestamp `db:"ts"`
	IsDeleted    bool               `db:"is_deleted"`
	CreatedAt    time.Time          `db:"created_at"`
	UpdatedAt    time.Time          `db:"updated_at"`
}
