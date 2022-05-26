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

func getMock(t *testing.T) (sqlmock.Sqlmock, TableCatalog) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error was not expected when opening a stub database connection: %s", err)
	}
	sqlxDB := sqlx.NewDb(db, "sqlmock")
	tc := TableCatalog{
		DB: sqlxDB,
	}
	return mock, tc
}

func TestCreateCollection(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	ddOp, _ := metastore.ToDdOperation(&internalpb.CreateCollectionRequest{
		CollectionName: collName,
		PartitionName:  partName,
		CollectionID:   collID,
		PartitionID:    partID,
	}, "CreateCollection")
	meta := map[string]interface{}{}
	meta[metastore.DDMsgSendPrefix] = false
	meta[metastore.DDOperationPrefix] = ddOp
	collInfo.Extra = meta

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into field_schemas").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into partitions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err := tc.CreateCollection(context.TODO(), collInfo, ts); err != nil {
		t.Errorf("error was not expected while creating collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateCollection_RollbackOnFailure1(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	if err := tc.CreateCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateCollection_RollbackOnFailure2(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into field_schemas").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	if err := tc.CreateCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateCollection_RollbackOnFailure3(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into field_schemas").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into partitions").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	if err := tc.CreateCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateCollection_RollbackOnFailure4(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into field_schemas").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into partitions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	if err := tc.CreateCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetCollectionByID(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"
	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.GetCollectionByID(context.TODO(), collID, ts)
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	aliasesBytes, _ := json.Marshal([]string{"a", "b"})
	collTimestamp := 433286016553713676
	var partCreatedTimestamp uint64 = 433286016527499280
	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias", "c.description", "c.auto_id", "c.ts", "c.properties", "c.is_deleted",
			"p.id", "p.partition_id", "p.partition_name", "p.partition_created_timestamp", "p.collection_id", "p.ts", "p.is_deleted", "p.created_at", "p.updated_at",
			"f.id", "f.field_id", "f.field_name", "f.collection_id",
			"i.id", "i.field_id", "i.collection_id", "i.index_id", "i.index_name", "i.index_params",
		},
	).AddRow([]driver.Value{1, tenantID, collID, collName, string(aliasesBytes), "", false, collTimestamp, "{}", false,
		10, partID, partName, partCreatedTimestamp, collID, 433286016461963275, false, time.Now(), time.Now(),
		1000, fieldID, fieldName, collID,
		1000, fieldID, collID, indexID, indexName, nil}...)
	mock.ExpectQuery(sqlSelectSql).WithArgs(collID, ts).WillReturnRows(rows)
	res, err := tc.GetCollectionByID(context.TODO(), collID, ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if res.CollectionID != collID {
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

func TestGetCollectionIDByName(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"
	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.GetCollectionIDByName(context.TODO(), collName, ts)
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	rows := sqlmock.NewRows(
		[]string{"collection_id"},
	).AddRow([]driver.Value{collID}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	collectionID, err := tc.GetCollectionIDByName(context.TODO(), collName, ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if collectionID != collID {
		t.Fatalf("unexpected collection_id:%d", collectionID)
	}
}

func TestGetCollectionByName(t *testing.T) {

}

func TestListCollections(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"
	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.ListCollections(context.TODO(), ts)
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	aliasesBytes, _ := json.Marshal([]string{"a", "b"})
	collTimestamp := 433286016553713676
	var partCreatedTimestamp uint64 = 433286016527499280
	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias", "c.description", "c.auto_id", "c.ts", "c.properties", "c.is_deleted",
			"p.id", "p.partition_id", "p.partition_name", "p.partition_created_timestamp", "p.collection_id", "p.ts", "p.is_deleted", "p.created_at", "p.updated_at",
			"f.id", "f.field_id", "f.field_name"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, string(aliasesBytes), "", false, collTimestamp, "{}", false,
		10, partID, partName, partCreatedTimestamp, collID, 433286016461963275, false, time.Now(), time.Now(),
		1000, fieldID, fieldName}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err := tc.ListCollections(context.TODO(), ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	var coll *model.Collection
	if v, ok := res[collName]; !ok {
		t.Fatalf("unexpected collection map")
	} else {
		coll = v
	}
	if coll.CollectionID != collID {
		t.Fatalf("unexpected collection_id:%d", coll.CollectionID)
	}
	if coll.Name != collName {
		t.Fatalf("unexpected collection_name:%s", coll.Name)
	}
	if coll.AutoID != false {
		t.Fatalf("unexpected auto_id:%t", coll.AutoID)
	}
	for _, val := range coll.Aliases {
		if val != "a" && val != "b" {
			t.Fatalf("unexpected collection_alias:%s", coll.Aliases)
		}
	}
	for _, pt := range coll.Partitions {
		if pt.PartitionID != partID && pt.PartitionName != partName && pt.PartitionCreatedTimestamp != partCreatedTimestamp {
			t.Fatalf("unexpected collection partitions")
		}
	}
	for _, field := range coll.Fields {
		if field.FieldID != fieldID && field.Name != fieldName {
			t.Fatalf("unexpected collection fields")
		}
	}
}

func TestCollectionExists(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"
	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	b := tc.CollectionExists(context.TODO(), collID, ts)
	if b {
		t.Fatalf("unexpected result:%t", b)
	}

	// mock select normal
	rows := sqlmock.NewRows(
		[]string{"tenant_id", "collection_id", "collection_name", "ts"},
	).AddRow([]driver.Value{tenantID, collID, collName, ts}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	b = tc.CollectionExists(context.TODO(), collID, ts)
	if !b {
		t.Fatalf("unexpected result:%t", b)
	}
}

func TestDropCollection(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	ddOp, _ := metastore.ToDdOperation(&internalpb.CreateCollectionRequest{
		CollectionName: collName,
		PartitionName:  partName,
		CollectionID:   collID,
		PartitionID:    partID,
	}, "DropCollection")
	meta := map[string]interface{}{}
	meta[metastore.DDMsgSendPrefix] = false
	meta[metastore.DDOperationPrefix] = ddOp
	collInfo.Extra = meta

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err != nil {
		t.Errorf("error was not expected while dropping collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDropCollection_RollbackOnFailure1(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sql := "update collections"

	mock.ExpectBegin()
	mock.ExpectExec(sql).WillReturnError(fmt.Errorf("update error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec(sql).WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropCollection(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropCollection_RollbackOnFailure2(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnError(fmt.Errorf("update error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropCollection(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropCollection_RollbackOnFailure3(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnError(fmt.Errorf("update error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropCollection(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropCollection_RollbackOnFailure4(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnError(fmt.Errorf("update error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropCollection(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropCollection_RollbackOnFailure5(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnError(fmt.Errorf("update error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropCollection(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropCollection_RollbackOnFailure6(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropCollection(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestCreatePartition(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	insertSql := "insert into partitions"

	// mock normal
	mock.ExpectExec(insertSql).WillReturnResult(sqlmock.NewResult(1, 2))
	if err := tc.CreatePartition(context.TODO(), collInfo, ts); err != nil {
		t.Errorf("error was not expected while creating collection: %s", err)
	}

	// mock update error
	errMsg := "insert sql failed"
	mock.ExpectExec(insertSql).WillReturnError(errors.New(errMsg))
	err := tc.CreatePartition(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropPartition(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	updateSql := "update partitions"

	// mock normal
	mock.ExpectExec(updateSql).WillReturnResult(sqlmock.NewResult(1, 1))
	if err := tc.DropPartition(context.TODO(), nil, partID, ts); err != nil {
		t.Errorf("error was not expected while dropping collection: %s", err)
	}

	// mock update error
	errMsg := "update sql failed"
	mock.ExpectExec(updateSql).WillReturnError(errors.New(errMsg))
	err := tc.DropPartition(context.TODO(), nil, partID, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock sql result error
	errMsg = "get sql RowsAffected failed"
	mock.ExpectExec(updateSql).WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	err = tc.DropPartition(context.TODO(), nil, partID, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock update RowsAffected is not 1
	//mock.ExpectExec(updateSql).WillReturnResult(sqlmock.NewResult(0, 0))
	//err = tc.DropPartition(context.TODO(), nil, partID, ts)
	//if !strings.Contains(err.Error(), "RowsAffected is not 1") {
	//    t.Fatalf("unexpected error:%s", err)
	//}
}

func TestCreateIndex(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into segment_indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	index := &model.Index{
		FieldID:   fieldID,
		IndexID:   indexID,
		IndexName: indexName,
	}

	var segmentIndexes []model.SegmentIndex
	segmentIndex := model.SegmentIndex{
		Segment: model.Segment{
			SegmentID: segID,
		},
		EnableIndex: false,
	}
	segmentIndexes = append(segmentIndexes, segmentIndex)
	index.SegmentIndexes = segmentIndexes

	// now we execute our method
	if err := tc.CreateIndex(context.TODO(), nil, index); err != nil {
		t.Errorf("error was not expected while creating collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateIndex_RollbackOnFailure1(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into indexes").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	index := &model.Index{
		FieldID:   fieldID,
		IndexID:   indexID,
		IndexName: indexName,
	}
	if err := tc.CreateIndex(context.TODO(), nil, index); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}
}

func TestCreateIndex_RollbackOnFailure2(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("insert into indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into segment_indexes").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	index := &model.Index{
		FieldID:   fieldID,
		IndexID:   indexID,
		IndexName: indexName,
	}
	if err := tc.CreateIndex(context.TODO(), nil, index); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}
}

func TestDropIndex(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err := tc.DropIndex(context.TODO(), nil, indexID, ts); err != nil {
		t.Errorf("error was not expected while dropping collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDropIndex_RollbackOnFailure1(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update indexes").WillReturnError(fmt.Errorf("delete error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropIndex(context.TODO(), nil, indexID, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropIndex(context.TODO(), nil, indexID, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropIndex_RollbackOnFailure2(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnError(fmt.Errorf("delete error"))
	mock.ExpectRollback()
	// now we execute our method
	if err := tc.DropIndex(context.TODO(), nil, indexID, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	errMsg := "get sql RowsAffected failed"
	mock.ExpectBegin()
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))
	mock.ExpectRollback()
	// mock sql result error
	err := tc.DropIndex(context.TODO(), nil, indexID, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestListIndexes(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"

	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.ListIndexes(context.TODO())
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	rows := sqlmock.NewRows(
		[]string{"id", "field_id", "collection_id", "index_id", "index_name", "index_params",
			"id", "partition_id", "segment_id", "field_id", "index_id", "build_id", "enable_index", "index_file_paths", "index_size"},
	).AddRow([]driver.Value{1, fieldID, collID, indexID, indexName, "",
		10, partID, segID, fieldID, indexID, buildID, false, "", 100}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err := tc.ListIndexes(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	idx := res[0]
	if idx.CollectionID != collID {
		t.Fatalf("unexpected collection_id:%d", idx.CollectionID)
	}
	if idx.FieldID != fieldID {
		t.Fatalf("unexpected field_id:%d", idx.FieldID)
	}
	if idx.IndexID != indexID {
		t.Fatalf("unexpected index_id:%d", idx.IndexID)
	}
	if idx.IndexName != indexName {
		t.Fatalf("unexpected index_name:%s", idx.IndexName)
	}
	for _, segIndex := range idx.SegmentIndexes {
		if segIndex.SegmentID != segID {
			t.Fatalf("unexpected segment_id:%d", segIndex.SegmentID)
		}
		if segIndex.PartitionID != partID {
			t.Fatalf("unexpected partition_id:%d", segIndex.PartitionID)
		}
		if segIndex.BuildID != buildID {
			t.Fatalf("unexpected build_id:%d", segIndex.BuildID)
		}
		if segIndex.EnableIndex != false {
			t.Fatalf("unexpected enable_index:%t", segIndex.EnableIndex)
		}
	}
}

func TestCreateAlias(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, nil}...)
	mock.ExpectQuery("select").WithArgs(collID, ts).WillReturnRows(rows)
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))

	// now we execute our request
	coll := &model.Collection{
		CollectionID: collID,
		Aliases:      []string{aliasName1, aliasName2},
	}
	err := tc.AddAlias(context.TODO(), coll, ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreateAlias_ReturnError1(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, nil}...)
	mock.ExpectQuery("select").WithArgs(collID, ts).WillReturnRows(rows)
	mock.ExpectExec("update collections").WillReturnError(errors.New("update error"))

	// now we execute our request
	coll := &model.Collection{
		CollectionID: collID,
		Aliases:      []string{aliasName1, aliasName2},
	}
	err := tc.AddAlias(context.TODO(), coll, ts)
	if !strings.Contains(err.Error(), "update error") {
		t.Fatalf("unexpected error:%s", err)
	}
}
func TestCreateAlias_ReturnError2(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, nil}...)
	errMsg := "get sql RowsAffected failed"
	mock.ExpectQuery("select").WithArgs(collID, ts).WillReturnRows(rows)
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewErrorResult(errors.New(errMsg)))

	// mock sql result error
	coll := &model.Collection{
		CollectionID: collID,
		Aliases:      []string{aliasName1, aliasName2},
	}
	err := tc.AddAlias(context.TODO(), coll, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropAlias(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	aliasesBytes, _ := json.Marshal([]string{aliasName1, aliasName2})
	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, string(aliasesBytes)}...)
	mock.ExpectQuery("select").WithArgs(collID, ts).WillReturnRows(rows)
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))

	// now we execute our request
	err := tc.DropAlias(context.TODO(), collID, aliasName1, ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDropAlias_Error(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	aliasesBytes, _ := json.Marshal([]string{aliasName1, aliasName2})
	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, string(aliasesBytes)}...)
	mock.ExpectQuery("select").WithArgs(collID, ts).WillReturnRows(rows)
	mock.ExpectExec("update collections").WillReturnError(errors.New("update error"))

	// now we execute our request
	err := tc.DropAlias(context.TODO(), collID, aliasName1, ts)
	if !strings.Contains(err.Error(), "update error") {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestAlterAlias(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	rows := sqlmock.NewRows(
		[]string{"c.id", "c.tenant_id", "c.collection_id", "c.collection_name", "c.collection_alias"},
	).AddRow([]driver.Value{1, tenantID, collID, collName, nil}...)
	mock.ExpectQuery("select").WithArgs(collID, ts).WillReturnRows(rows)
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(0, 1))

	// now we execute our request
	coll := &model.Collection{
		CollectionID: collID,
		Aliases:      []string{aliasName1, aliasName2},
	}
	err := tc.AddAlias(context.TODO(), coll, ts)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestListAliases(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"

	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.ListAliases(context.TODO())
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	aliaesBytes, _ := json.Marshal([]string{aliasName1, aliasName2})
	rows := sqlmock.NewRows(
		[]string{"collection_id", "collection_alias"},
	).AddRow([]driver.Value{1, string(aliaesBytes)}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err := tc.ListAliases(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if len(res) != 2 {
		t.Fatalf("unexpected result:%d", len(res))
	}
	for _, coll := range res {
		if coll.CollectionID != collID {
			t.Fatalf("unexpected collection_id:%d", coll.CollectionID)
		}
		if coll.Name != aliasName1 && coll.Name != aliasName2 {
			t.Fatalf("unexpected field_id:%s", coll.Name)
		}
	}

	// mock alias is nil
	rows = sqlmock.NewRows(
		[]string{"collection_id", "collection_alias"},
	).AddRow([]driver.Value{1, nil}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err = tc.ListAliases(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock alias is empty
	rows = sqlmock.NewRows(
		[]string{"collection_id", "collection_alias"},
	).AddRow([]driver.Value{1, ""}...)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err = tc.ListAliases(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestGetCredential(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"

	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.GetCredential(context.TODO(), "Alice")
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	rows := sqlmock.NewRows(
		[]string{"tenant_id", "username", "encrypted_password", "is_super", "is_deleted"},
	).AddRow([]driver.Value{tenantID, "Alice", "$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2", false, false}...)
	mock.ExpectQuery(sqlSelectSql).WithArgs("Alice").WillReturnRows(rows)
	credential, err := tc.GetCredential(context.TODO(), "Alice")
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if credential.Username != "Alice" {
		t.Fatalf("unexpected username:%s", credential.Username)
	}
	if credential.EncryptedPassword != "$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2" {
		t.Fatalf("unexpected password:%s", credential.EncryptedPassword)
	}
}

func TestCreateCredential(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	// mock select failure
	mock.ExpectExec("insert").WillReturnError(errors.New("insert error"))
	credential := &model.Credential{
		Username:          "Alice",
		EncryptedPassword: "$2a$10$3H9DLiHyPxJ29bMWRNyueOrGkbzJfE3BAR159ju3UetytAoKk7Ne2",
	}
	err := tc.CreateCredential(context.TODO(), credential)
	if !strings.Contains(err.Error(), "insert error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// now we execute our request
	mock.ExpectExec("insert into credential_users").WillReturnResult(sqlmock.NewResult(1, 1))
	err = tc.CreateCredential(context.TODO(), credential)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDropCredential(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	// mock select failure
	mock.ExpectExec("update").WillReturnError(errors.New("update error"))
	err := tc.DropCredential(context.TODO(), "Alice")
	if !strings.Contains(err.Error(), "update error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// now we execute our request
	mock.ExpectExec("update credential_users").WillReturnResult(sqlmock.NewResult(1, 1))
	err = tc.DropCredential(context.TODO(), "Alice")
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock sql result error
	mock.ExpectExec("update credential_users").WillReturnResult(sqlmock.NewErrorResult(errors.New("update error")))
	err = tc.DropCredential(context.TODO(), "Alice")
	if !strings.Contains(err.Error(), "update error") {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestListCredentials(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"

	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.ListCredentials(context.TODO())
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	rows := sqlmock.NewRows(
		[]string{"username"},
	).AddRow("Alice").AddRow("Bob")
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	res, err := tc.ListCredentials(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if len(res) != 2 {
		t.Fatalf("unexpected result:%d", len(res))
	}
	for _, uname := range res {
		if uname != "Alice" && uname != "Bob" {
			t.Fatalf("unexpected username:%s", uname)
		}
	}
}

func TestLoadDdOperation(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	sqlSelectSql := "select"

	// mock select failure
	mock.ExpectQuery(sqlSelectSql).WillReturnError(errors.New("select error"))
	_, err := tc.LoadDdOperation(context.TODO())
	if !strings.Contains(err.Error(), "select error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// mock select normal
	rows := sqlmock.NewRows(
		[]string{"operation_type", "operation_body", "is_sent"},
	).AddRow("CreateCollection", "xxx", false)
	mock.ExpectQuery(sqlSelectSql).WillReturnRows(rows)
	ddOp, err := tc.LoadDdOperation(context.TODO())
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}

	if ddOp.Type != "CreateCollection" {
		t.Fatalf("unexpected dd type:%s", ddOp.Type)
	}
	if ddOp.IsSent != false {
		t.Fatalf("unexpected result:%t", ddOp.IsSent)
	}
}

func TestUpdateDDOperation(t *testing.T) {
	mock, tc := getMock(t)
	defer tc.DB.Close()

	ddOp, _ := metastore.ToDdOperation(&internalpb.CreateCollectionRequest{
		CollectionName: collName,
		PartitionName:  partName,
		CollectionID:   collID,
		PartitionID:    partID,
	}, "CreateCollection")

	// mock select failure
	mock.ExpectExec("update").WillReturnError(errors.New("update error"))
	err := tc.UpdateDDOperation(context.TODO(), ddOp, true)
	if !strings.Contains(err.Error(), "update error") {
		t.Fatalf("unexpected error:%s", err)
	}

	// now we execute our request
	mock.ExpectExec("update").WillReturnResult(sqlmock.NewResult(0, 1))
	err = tc.UpdateDDOperation(context.TODO(), ddOp, true)
	if err != nil {
		t.Fatalf("unexpected error:%s", err)
	}
}
