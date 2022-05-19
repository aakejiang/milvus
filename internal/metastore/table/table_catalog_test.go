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
	_, err = tc.GetCollectionIDByName(context.TODO(), collName, ts)
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

func TestListCollections(t *testing.T) {
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
	_, err = tc.ListCollections(context.TODO(), ts)
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
	b := tc.CollectionExists(context.TODO(), collID, ts)
	if b {
		t.Fatalf("unexpected error:%s", err)
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
	}, "DropCollection")
	meta := map[string]string{}
	meta[metastore.DDMsgSendPrefix] = "false"
	meta[metastore.DDOperationPrefix] = ddOpStr
	collInfo.Extra = meta

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err = tc.DropCollection(context.TODO(), collInfo, ts); err != nil {
		t.Errorf("error was not expected while dropping collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

// a failing test case
func TestDropCollection_RollbackOnFailure(t *testing.T) {
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
	}, "DropCollection")
	meta := map[string]string{}
	meta[metastore.DDMsgSendPrefix] = "false"
	meta[metastore.DDOperationPrefix] = ddOpStr
	collInfo.Extra = meta

	mock.ExpectBegin()
	mock.ExpectExec("update collections").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update partitions").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update field_schemas").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("insert into dd_msg_send").WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	// now we execute our method
	if err = tc.DropCollection(context.TODO(), collInfo, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestCreatePartition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	tc := TableCatalog{
		DB: sqlxDB,
	}

	insertSql := "insert into partitions"

	// mock normal
	mock.ExpectExec(insertSql).WillReturnResult(sqlmock.NewResult(1, 2))
	if err = tc.CreatePartition(context.TODO(), collInfo, ts); err != nil {
		t.Errorf("error was not expected while creating collection: %s", err)
	}

	// mock update error
	errMsg := "insert sql failed"
	mock.ExpectExec(insertSql).WillReturnError(errors.New(errMsg))
	err = tc.CreatePartition(context.TODO(), collInfo, ts)
	if !strings.Contains(err.Error(), errMsg) {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestDropPartition(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	tc := TableCatalog{
		DB: sqlxDB,
	}

	updateSql := "update partitions"

	// mock normal
	mock.ExpectExec(updateSql).WillReturnResult(sqlmock.NewResult(1, 1))
	if err = tc.DropPartition(context.TODO(), nil, partID, ts); err != nil {
		t.Errorf("error was not expected while dropping collection: %s", err)
	}

	// mock update error
	errMsg := "update sql failed"
	mock.ExpectExec(updateSql).WillReturnError(errors.New(errMsg))
	err = tc.DropPartition(context.TODO(), nil, partID, ts)
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
	mock.ExpectExec(updateSql).WillReturnResult(sqlmock.NewResult(0, 0))
	err = tc.DropPartition(context.TODO(), nil, partID, ts)
	if !strings.Contains(err.Error(), "RowsAffected is not 1") {
		t.Fatalf("unexpected error:%s", err)
	}
}

func TestCreateIndex(t *testing.T) {
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
	if err = tc.CreateIndex(context.TODO(), nil, index); err != nil {
		t.Errorf("error was not expected while creating collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDropIndex(t *testing.T) {
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
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update segment_indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	// now we execute our method
	if err = tc.DropIndex(context.TODO(), nil, indexID, ts); err != nil {
		t.Errorf("error was not expected while dropping collection: %s", err)
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDropIndex_RollbackOnFailure(t *testing.T) {
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
	mock.ExpectExec("update indexes").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("update segment_indexes").WillReturnError(fmt.Errorf("delete error"))
	mock.ExpectRollback()

	// now we execute our method
	if err = tc.DropIndex(context.TODO(), nil, indexID, ts); err == nil {
		t.Errorf("was expecting an error, but there was none")
	}

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}
