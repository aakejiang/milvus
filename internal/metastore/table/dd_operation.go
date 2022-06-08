package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type DdOperation struct {
	Id            int64               `db:"id"`
	TenantID      *string             `db:"tenant_id"`
	OperationType string              `db:"operation_type"`
	OperationBody string              `db:"operation_body"`
	IsSent        bool                `db:"is_sent"`
	Ts            *typeutil.Timestamp `db:"ts"`
	CreatedAt     time.Time           `db:"created_at"`
	UpdatedAt     time.Time           `db:"updated_at"`
}
