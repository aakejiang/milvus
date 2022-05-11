package table

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Field struct {
	FieldID      int64                    `db:"field_id"`
	FieldName    string                   `db:"field_name"`
	IsPrimaryKey bool                     `db:"is_primary_key"`
	Description  string                   `db:"description"`
	DataType     schemapb.DataType        `db:"data_type"`
	TypeParams   []*commonpb.KeyValuePair `db:"type_params"`
	IndexParams  []*commonpb.KeyValuePair `db:"index_params"`
	AutoID       bool                     `db:"auto_id"`
	CollectionID int64                    `db:"collection_id"`
	Timestamp    typeutil.Timestamp       `db:"ts"`
}
