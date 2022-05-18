package table

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	tenantID        = "tenant1"
	collName        = "testColl"
	collNameInvalid = "testColl_invalid"
	aliasName1      = "alias1"
	aliasName2      = "alias2"
	collID          = typeutil.UniqueID(1)
	collIDInvalid   = typeutil.UniqueID(2)
	partIDDefault   = typeutil.UniqueID(10)
	partNameDefault = "_default"
	partID          = typeutil.UniqueID(20)
	partName        = "testPart"
	partIDInvalid   = typeutil.UniqueID(21)
	segID           = typeutil.UniqueID(100)
	segID2          = typeutil.UniqueID(101)
	fieldID         = typeutil.UniqueID(110)
	fieldName       = "field_110"
	fieldID2        = typeutil.UniqueID(111)
	indexID         = typeutil.UniqueID(10000)
	indexID2        = typeutil.UniqueID(10001)
	buildID         = typeutil.UniqueID(201)
	indexName       = "testColl_index_110"
)

var collInfo = &model.Collection{
	CollectionID: collID,
	Name:         collName,
	AutoID:       false,
	Fields: []*model.Field{
		{
			FieldID:      fieldID,
			Name:         "field110",
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "field110-k1",
					Value: "field110-v1",
				},
				{
					Key:   "field110-k2",
					Value: "field110-v2",
				},
			},
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "field110-i1",
					Value: "field110-v1",
				},
				{
					Key:   "field110-i2",
					Value: "field110-v2",
				},
			},
		},
	},
	FieldIndexes: []*model.Index{
		{
			FieldID: fieldID,
			IndexID: indexID,
		},
	},
	CreateTime: 0,
	Partitions: []*model.Partition{
		{
			PartitionID:               partIDDefault,
			PartitionName:             partNameDefault,
			PartitionCreatedTimestamp: 0,
		},
	},
}

var vtso typeutil.Timestamp
var ftso = func() typeutil.Timestamp {
	vtso++
	return vtso
}
var ts = ftso()

func TestCreateCollection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	tc := TableCatalog{
		DB: sqlxDB,
	}

	ddOpStr, _ := metastore.EncodeDdOperation(&internalpb.CreateCollectionRequest{
		CollectionName: collName,
		PartitionName:  partName,
		CollectionID:   collID,
		PartitionID:    partID,
	}, "CreateCollection")
	meta := map[string]string{}
	meta[metastore.DDMsgSendPrefix] = "false"
	meta[metastore.DDOperationPrefix] = ddOpStr
	collInfo.Extra = meta

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into field_schemas").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into partitions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err = tc.CreateCollection(context.TODO(), collInfo, ts); err != nil {
		t.Errorf("error was not expected while creating collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// a failing test case
func TestCreateCollection_RollbackOnFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	tc := TableCatalog{
		DB: sqlxDB,
	}

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into field_schemas").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	if err = tc.CreateCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetCollectionByID(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock database: %s", err)
	}
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	tc := TableCatalog{
		DB: sqlxDB,
	}

	sqlSelectSql := "select"

	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err = tc.GetCollectionByID(context.TODO(), collID, ts)
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	aliasesStr, _ := json.Marshal([]string{"a", "b"})
	collTimestamp := 433286016553713676
	var partCreatedTimestamp uint64 = 433286016527499280
	rows := sqlmock.NewRows(
		[]string{"id", "tenant_id", "collection_id", "collection_name", "collection_alias", "description", "auto_id", "ts", "properties", "is_deleted",
			"id", "partition_id", "partition_name", "partition_created_timestamp", "collection_id", "ts", "is_deleted", "created_at", "updated_at",
			"id", "field_id", "field_name"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, string(aliasesStr), "", false, collTimestamp, "{}", false,
		10, partID, partName, partCreatedTimestamp, collID, 433286016461963275, false, time.Now(), time.Now(),
		1000, fieldID, fieldName}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err := tc.GetCollectionByID(context.TODO(), collID, ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if res.CollectionID != int64(collID) {
		t.Fatalf("unexpected collection_id:%d", res.CollectionID)
	}
	if res.Name != collName {
		t.Fatalf("unexpected collection_name:%s", res.Name)
	}
	if res.AutoID != false {
		t.Fatalf("unexpected auto_id:%t", res.AutoID)
	}
	for _, val := range res.Aliases {
		if val != "a" && val != "b" {
			t.Fatalf("unexpected collection_alias:%s", res.Aliases)
		}
	}
	for _, pt := range res.Partitions {
		if pt.PartitionID != partID && pt.PartitionName != partName && pt.PartitionCreatedTimestamp != partCreatedTimestamp {
			t.Fatalf("unexpected collection partitions")
		}
	}
	for _, field := range res.Fields {
		if field.FieldID != fieldID && field.Name != fieldName {
			t.Fatalf("unexpected collection fields")
		}
	}
}
