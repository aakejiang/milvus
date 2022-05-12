package table

import (
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Field struct {
	Id           int64              `db:"id"`
	FieldID      int64              `db:"field_id"`
	FieldName    string             `db:"field_name"`
	IsPrimaryKey bool               `db:"is_primary_key"`
	Description  string             `db:"description"`
	DataType     schemapb.DataType  `db:"data_type"`
	TypeParams   string             `db:"type_params"`
	IndexParams  string             `db:"index_params"`
	AutoID       bool               `db:"auto_id"`
	CollectionID int64              `db:"collection_id"`
	Timestamp    typeutil.Timestamp `db:"ts"`
	IsDeleted    bool               `db:"is_deleted"`
	CreatedAt    typeutil.Timestamp `db:"created_at"`
	UpdatedAt    typeutil.Timestamp `db:"updated_at"`
}
