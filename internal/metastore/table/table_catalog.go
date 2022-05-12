package table

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type TableCatalog struct {
	DB *sqlx.DB
}

func (tc *TableCatalog) CreateCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	// start transaction
	tx, err := tc.DB.Beginx()
	if err != nil {
		log.Error("begin transaction failed", zap.Error(err))
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// sql 1
	sqlStr1 := "insert into collections(tenant_id, collection_id, collection_name, description, auto_id, ts, properties) values (?,?,?,?,?,?,?)"
	properties := CollProperties{
		VirtualChannelNames:  collection.VirtualChannelNames,
		PhysicalChannelNames: collection.PhysicalChannelNames,
		ShardsNum:            collection.ShardsNum,
		StartPositions:       collection.StartPositions,
		ConsistencyLevel:     collection.ConsistencyLevel,
	}
	propertiesStr, err := json.Marshal(properties)
	if err != nil {
		log.Error("marshal collection properties error", zap.Error(err))
		return err
	}
	_, err = tx.Exec(sqlStr1, collection.TenantID, collection.CollectionID, collection.Name, collection.Description, collection.AutoID, ts, propertiesStr)
	if err != nil {
		log.Error("insert collection failed", zap.Error(err))
		return err
	}

	// sql 2
	sqlStr2 := "insert into field_schemas(field_id, field_name, is_primary_key, description, data_type, type_params, index_params, auto_id, collection_id, ts) values (:field_id, :field_name, :is_primary_key, :description, :data_type, :type_params, :index_params, :auto_id, :collection_id, :ts)"
	var fields []Field
	for _, field := range collection.Fields {
		f := Field{
			FieldID:      field.FieldID,
			FieldName:    field.Name,
			IsPrimaryKey: field.IsPrimaryKey,
			Description:  field.Description,
			DataType:     field.DataType,
			TypeParams:   field.TypeParams,
			IndexParams:  field.IndexParams,
			AutoID:       field.AutoID,
			CollectionID: collection.CollectionID,
			Timestamp:    ts,
		}
		fields = append(fields, f)
	}
	_, err = tx.NamedExec(sqlStr2, fields)
	if err != nil {
		log.Error("batch insert field_schemas failed", zap.Error(err))
		return err
	}

	// sql 3
	sqlStr3 := "insert into partitions(partition_id, partition_name, partition_created_timestamp, collection_id, ts) values (:partition_id, :partition_name, :partition_created_timestamp, :collection_id, :ts)"
	var partitions []Partition
	for _, partition := range collection.Partitions {
		p := Partition{
			PartitionID:               partition.PartitionID,
			PartitionName:             partition.PartitionName,
			PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
			CollectionID:              collection.CollectionID,
			Timestamp:                 ts,
		}
		partitions = append(partitions, p)
	}
	_, err = tx.NamedExec(sqlStr3, partitions)
	if err != nil {
		log.Error("batch insert partitions failed", zap.Error(err))
		return err
	}

	// sql 4
	sqlStr4 := "insert into dd_msg_send(operation_type, operation_body, is_sent, ts) values (?,?,?,?)"
	ddOpStr := collection.Extra[metastore.DDOperationPrefix]
	var ddOp metastore.DdOperation
	err = metastore.DecodeDdOperation(ddOpStr, &ddOp)
	if err != nil {
		log.Error("decode DD operation failed", zap.Error(err))
		return err
	}
	isDDMsgSent := "false"
	_, err = tx.Exec(sqlStr4, ddOp.Type, ddOpStr, isDDMsgSent, ts)
	if err != nil {
		log.Error("insert dd_msg_send failed", zap.Error(err))
		return err
	}

	return nil
}

func (tc *TableCatalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	sqlStr := "select c.*, f.*, p.*, i.* " +
		" from collections c " +
		" join field_schemas f on c.collection_id = f.collection_id and c.ts = f.ts" +
		" join partitions p on c.collection_id = p.collection_id and c.ts = p.ts" +
		" join indexes_builder i on c.collection_id = i.collection_id " +
		" where c.is_deleted=false and f.is_deleted=false and p.is_deleted=false and i.is_deleted=false and c.collection_id=? and c.ts=?"
	var result []struct {
		Collection
		Partition
		Field
		IndexBuilder
	}
	err := tc.DB.Select(&result, sqlStr, collectionID, ts)
	if err != nil {
		log.Error("get collection by id failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	var colls []*model.Collection
	for _, record := range result {
		c := ConvertCollectionDBToModel(&record.Collection, &record.Partition, &record.Field, &record.IndexBuilder)
		colls = append(colls, c)
	}
	collMap := ConvertCollectionsToIDMap(colls)
	if _, ok := collMap[collectionID]; !ok {
		log.Error("not found collection in the map", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, fmt.Errorf("not found collection %d in the map", collectionID)
	}
	return collMap[collectionID], nil
}

func (tc *TableCatalog) GetCollectionIDByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	sqlStr := "select collection_id from collections where is_deleted=false and collection_name=? and ts=?"
	var collID typeutil.UniqueID
	err := tc.DB.Get(collID, sqlStr, collectionName, ts)
	if err != nil {
		log.Error("get collection id by name failed", zap.String("collName", collectionName), zap.Uint64("ts", ts), zap.Error(err))
		return 0, err
	}
	return collID, nil
}

func (tc *TableCatalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	collectionID, err := tc.GetCollectionIDByName(ctx, collectionName, ts)
	if err != nil {
		log.Error("get collection id by name failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}
	return tc.GetCollectionByID(ctx, collectionID, ts)
}

func (tc *TableCatalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	sqlStr := "select c.*, f.*, p.*, i.* " +
		" from collections c " +
		" join field_schemas f on c.collection_id = f.collection_id and c.ts = f.ts" +
		" join partitions p on c.collection_id = p.collection_id and c.ts = p.ts" +
		" join indexes_builder i on c.collection_id = i.collection_id" +
		" where c.is_deleted=false and f.is_deleted=false and p.is_deleted=false and i.is_deleted=false and c.ts=?"
	var result []struct {
		Collection
		Partition
		Field
		IndexBuilder
	}
	err := tc.DB.Select(&result, sqlStr, ts)
	if err != nil {
		log.Error("list collection failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	var colls []*model.Collection
	for _, record := range result {
		c := ConvertCollectionDBToModel(&record.Collection, &record.Partition, &record.Field, &record.IndexBuilder)
		colls = append(colls, c)
	}
	return ConvertCollectionsToNameMap(colls), nil
}

func (tc *TableCatalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	sqlStr := "select tenant_id, collection_id, collection_name, description, auto_id, ts, properties, created_time from collections where is_deleted=false and collection_id=? and ts=?"
	var coll Collection
	err := tc.DB.Get(&coll, sqlStr, collectionID, ts)
	if err != nil {
		log.Error("get collection by ID failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		// Also, an error is returned if the result set is empty.
		return false
	}
	return true
}

func (tc *TableCatalog) ExecSqlWithTransaction(tx *sql.Tx, handle func(tx *sql.Tx) error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()
	return handle(tx)
}

func (tc *TableCatalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	// start transaction
	tx, err := tc.DB.Beginx()
	if err != nil {
		log.Error("begin transaction failed", zap.Error(err))
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	// sql 1
	sqlStr1 := "update collections set is_deleted=true where collection_id=?"
	rs, err := tx.Exec(sqlStr1, collectionInfo.CollectionID)
	if err != nil {
		log.Error("delete collection failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	if n != 1 {
		return errors.New("execute delete collection sql failed")
	}

	// sql 2
	sqlStr2 := "update partitions set is_deleted=true where collection_id=? and ts=?"
	rs, err = tx.Exec(sqlStr2, collectionInfo.CollectionID, ts)
	if err != nil {
		log.Error("delete partition failed", zap.Error(err))
		return err
	}
	n, err = rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table partitions RowsAffected:", zap.Any("rows", n))

	// sql 3
	sqlStr3 := "update field_schemas set is_deleted=true where collection_id=? and ts=?"
	rs, err = tx.Exec(sqlStr3, collectionInfo.CollectionID, ts)
	if err != nil {
		log.Error("delete field_schemas failed", zap.Error(err))
		return err
	}
	n, err = rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table field_schemas RowsAffected:", zap.Any("rows", n))

	// sql 4
	sqlStr4 := "update indexes_builder set is_deleted=true where collection_id=?"
	rs, err = tx.Exec(sqlStr4, collectionInfo.CollectionID)
	if err != nil {
		log.Error("update indexes_builder by collection ID failed", zap.Error(err))
		return err
	}
	n, err = rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table indexes_builder RowsAffected:", zap.Any("rows", n))

	// sql 5
	sqlStr5 := "insert into dd_msg_send(operation_type, operation_body, is_sent, ts) values (?,?,?,?)"
	ddOpStr := collectionInfo.Extra[metastore.DDOperationPrefix]
	var ddOp metastore.DdOperation
	err = metastore.DecodeDdOperation(ddOpStr, &ddOp)
	if err != nil {
		log.Error("decode DD operation failed", zap.Error(err))
		return err
	}
	isDDMsgSent := "false"
	_, err = tx.Exec(sqlStr5, ddOp.Type, ddOpStr, isDDMsgSent, ts)
	if err != nil {
		log.Error("insert dd_msg_send failed", zap.Error(err))
		return err
	}

	return err
}

func (tc *TableCatalog) CreatePartition(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	// sql 1
	sqlStr1 := "insert into partitions(partition_id, partition_name, partition_created_timestamp, collection_id, ts) values (:partition_id, :partition_name, :partition_created_timestamp, :collection_id, :ts)"
	var partitions []Partition
	for _, partition := range coll.Partitions {
		p := Partition{
			PartitionID:               partition.PartitionID,
			PartitionName:             partition.PartitionName,
			PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
			CollectionID:              coll.CollectionID,
			Timestamp:                 ts,
		}
		partitions = append(partitions, p)
	}
	_, err := tc.DB.NamedExec(sqlStr1, partitions)
	if err != nil {
		log.Error("batch insert partitions failed", zap.Error(err))
		return err
	}

	return nil
}

func (tc *TableCatalog) DropPartition(ctx context.Context, collection *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	sqlStr1 := "update partitions set is_deleted=true where partition_id=? and ts=?"
	rs, err := tc.DB.Exec(sqlStr1, partitionID, ts)
	if err != nil {
		log.Error("delete partition failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	if n != 1 {
		return errors.New("execute delete partition sql failed")
	}

	return nil
}

func (tc *TableCatalog) CreateIndex(ctx context.Context, index *model.Index) error {
	sqlStr := "insert into indexes_builder(field_id, enable_index, index_id, index_name, build_id, index_params, index_file_paths, index_size, collection_id) values (:field_id, :enable_index, :index_id, :index_name, :build_id, :index_params, :index_file_paths, :index_size, :collection_id)"
	fi := IndexBuilder{
		FieldID:        index.FieldID,
		IndexID:        index.IndexID,
		IndexName:      index.IndexName,
		BuildID:        index.BuildID,
		IndexParams:    index.IndexParams,
		IndexFilePaths: index.IndexFilePaths,
		IndexSize:      index.IndexSize,
		CollectionID:   index.CollectionID,
	}
	_, err := tc.DB.NamedExec(sqlStr, fi)
	if err != nil {
		log.Error("insert indexes_builder failed", zap.Error(err))
		return err
	}

	return nil
}

func (tc *TableCatalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	// sql 2
	sqlStr2 := "update indexes_builder set is_deleted=true where index_id=?"
	rs, err := tc.DB.Exec(sqlStr2, dropIdxID)
	if err != nil {
		log.Error("update indexes_builder by index ID failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	if n != 1 {
		return errors.New("execute update indexes_builder sql failed")
	}

	return nil
}

func (tc *TableCatalog) ListSegmentIndexes(ctx context.Context) ([]*model.Index, error) {
	sqlStr := "select collection_id, partition_id, segment_id, field_id, index_id, build_id, enable_index from indexes_builder where is_deleted=false"
	var indexes []*IndexBuilder
	err := tc.DB.Select(&indexes, sqlStr)
	if err != nil {
		log.Error("list indexes failed", zap.Error(err))
		return nil, err
	}
	return BatchConvertIndexDBToModel(indexes), nil
}

func (tc *TableCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	sqlStr := "select index_id, index_name, index_params from indexes_builder where is_deleted=false"
	var indexes []*IndexBuilder
	err := tc.DB.Select(&indexes, sqlStr)
	if err != nil {
		log.Error("list indexes failed", zap.Error(err))
		return nil, err
	}
	return BatchConvertIndexDBToModel(indexes), nil
}

func (tc *TableCatalog) CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	coll, err := tc.GetCollectionByID(ctx, collection.CollectionID, ts)
	if err != nil {
		log.Error("get collection by ID failed", zap.Int64("collectionID", collection.CollectionID))
		return err
	}
	presentAlias := coll.Aliases
	newAlias := collection.Aliases
	allAlias := metastore.ConvertInterfaceSlice(typeutil.SliceRemoveDuplicate(append(presentAlias, newAlias...)))
	allAliasStr, err := json.Marshal(allAlias)
	if err != nil {
		log.Error("marshal alias failed", zap.Error(err))
		return err
	}
	// sql 1
	sqlStr1 := "update collections set collection_alias=? where collection_id=? and ts=?"
	rs, err := tc.DB.Exec(sqlStr1, allAliasStr, collection.CollectionID, ts)
	if err != nil {
		log.Error("add alias failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	if n != 1 {
		return errors.New("execute add alias sql failed")
	}

	return nil
}

func (tc *TableCatalog) DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	coll, err := tc.GetCollectionByID(ctx, collectionID, ts)
	if err != nil {
		log.Error("get collection by ID failed", zap.Int64("collectionID", collectionID))
		return err
	}
	presentAlias := coll.Aliases
	for i, ele := range presentAlias {
		if ele == alias {
			presentAlias = append(presentAlias[:i], presentAlias[i+1:]...)
		}
	}
	aliasStr, err := json.Marshal(presentAlias)
	if err != nil {
		log.Error("marshal alias failed", zap.Error(err))
		return err
	}
	// sql 1
	sqlStr1 := "update collections set collection_alias=? where collection_id=? and ts=?"
	rs, err := tc.DB.Exec(sqlStr1, aliasStr, collectionID, ts)
	if err != nil {
		log.Error("drop alias failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	if n != 1 {
		return errors.New("execute drop alias sql failed")
	}

	return nil
}

func (tc *TableCatalog) AlterAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return tc.CreateAlias(ctx, collection, ts)
}

// ListAliases query collection ID and aliases only, other information are not needed
func (tc *TableCatalog) ListAliases(ctx context.Context) ([]*model.Collection, error) {
	sqlStr := "select collection_id, collection_alias from collections where is_deleted=false"
	var colls []*Collection
	err := tc.DB.Select(&colls, sqlStr)
	if err != nil {
		log.Error("list collection alias failed", zap.Error(err))
		return nil, err
	}

	var collAlias []*model.Collection
	for _, coll := range colls {
		var aliases []string
		err = json.Unmarshal([]byte(coll.CollectionAlias), aliases)
		if err != nil {
			log.Error("unmarshal collection alias failed", zap.Error(err))
			continue
		}
		for _, alias := range aliases {
			collAlias = append(collAlias, &model.Collection{
				CollectionID: coll.CollectionID,
				Name:         alias,
			})
		}
	}
	return collAlias, nil
}

func (tc *TableCatalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	sqlStr := "select tenant_id, username, encrypted_password, is_super, is_deleted from credential_users where is_deleted=false and username=?"
	var user *User
	err := tc.DB.Get(&user, sqlStr, username)
	if err != nil {
		log.Error("get credential user by username failed", zap.Error(err))
		return nil, err
	}
	return ConvertUserDBToModel(user), nil
}

func (tc *TableCatalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	sqlStr1 := "insert into credential_users(username, encrypted_password) values (:username, :encrypted_password)"
	user := User{
		Username:          credential.Username,
		EncryptedPassword: credential.EncryptedPassword,
	}
	_, err := tc.DB.NamedExec(sqlStr1, user)
	if err != nil {
		log.Error("insert credential user failed", zap.Error(err))
		return err
	}

	return nil
}

func (tc *TableCatalog) DropCredential(ctx context.Context, username string) error {
	sqlStr1 := "update credential_users set is_deleted=true where username=?"
	rs, err := tc.DB.Exec(sqlStr1, username)
	if err != nil {
		log.Error("delete credential_users failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	if n != 1 {
		return errors.New("execute delete credential_users sql failed")
	}
	return nil
}

func (tc *TableCatalog) ListCredentials(ctx context.Context) ([]string, error) {
	sqlStr := "select username from credential_users where is_deleted=false"
	var usernames []string
	err := tc.DB.Select(&usernames, sqlStr)
	if err != nil {
		log.Error("list credential usernames failed", zap.Error(err))
		return nil, err
	}
	return usernames, nil
}

func (tc *TableCatalog) Close() {

}
