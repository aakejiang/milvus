package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

type Index struct {
	Id           *int64                   `db:"id"`
	FieldID      *int64                   `db:"field_id"`
	CollectionID *int64                   `db:"collection_id"`
	IndexID      *int64                   `db:"index_id"`
	IndexName    *string                  `db:"index_name"`
	IndexParams  []*commonpb.KeyValuePair `db:"index_params"`
	IsDeleted    *bool                    `db:"is_deleted"`
	CreatedAt    *time.Time               `db:"created_at"`
	UpdatedAt    *time.Time               `db:"updated_at"`
}
