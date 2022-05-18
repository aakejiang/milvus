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
	sqlStr1 := "insert into collections(tenant_id, collection_id, collection_name, collection_alias, description, auto_id, ts, properties) values (?,?,?,?,?,?,?)"
	aliasesBytes, err := json.Marshal(collection.Aliases)
	aliasesStr := string(aliasesBytes)
	if err != nil {
		log.Error("marshal alias failed", zap.Error(err))
		return err
	}
	properties := ConvertToCollectionProperties(collection)
	propertiesBytes, err := json.Marshal(properties)
	propertiesStr := string(propertiesBytes)
	if err != nil {
		log.Error("marshal collection properties error", zap.Error(err))
		return err
	}
	_, err = tx.Exec(sqlStr1, collection.TenantID, collection.CollectionID, collection.Name, aliasesStr, collection.Description, collection.AutoID, ts, propertiesStr)
	if err != nil {
		log.Error("insert collection failed", zap.Error(err))
		return err
	}

	// sql 2
	sqlStr2 := "insert into field_schemas(field_id, field_name, is_primary_key, description, data_type, type_params, index_params, auto_id, collection_id, ts) values (:field_id, :field_name, :is_primary_key, :description, :data_type, :type_params, :index_params, :auto_id, :collection_id, :ts)"
	var fields []Field
	for _, field := range collection.Fields {
		typeParamsBytes, err := json.Marshal(field.TypeParams)
		if err != nil {
			log.Error("marshal TypeParams of field failed", zap.Error(err))
			continue
		}
		typeParamsStr := string(typeParamsBytes)
		indexParamsBytes, err := json.Marshal(field.IndexParams)
		if err != nil {
			log.Error("marshal IndexParams of field failed", zap.Error(err))
			continue
		}
		indexParamsStr := string(indexParamsBytes)
		f := Field{
			FieldID:      &field.FieldID,
			FieldName:    &field.Name,
			IsPrimaryKey: field.IsPrimaryKey,
			Description:  &field.Description,
			DataType:     field.DataType,
			TypeParams:   &typeParamsStr,
			IndexParams:  &indexParamsStr,
			AutoID:       field.AutoID,
			CollectionID: &collection.CollectionID,
			Ts:           ts,
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
			PartitionID:               &partition.PartitionID,
			PartitionName:             &partition.PartitionName,
			PartitionCreatedTimestamp: &partition.PartitionCreatedTimestamp,
			CollectionID:              &collection.CollectionID,
			Ts:                        ts,
		}
		partitions = append(partitions, p)
	}
	_, err = tx.NamedExec(sqlStr3, partitions)
	if err != nil {
		log.Error("batch insert partitions failed", zap.Error(err))
		return err
	}

	// sql 4
	if ddOpStr, ok := collection.Extra[metastore.DDOperationPrefix]; ok {
		sqlStr4 := "insert into dd_msg_send(operation_type, operation_body, is_sent, ts) values (?,?,?,?)"
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
	}

	return nil
}

func (tc *TableCatalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	sqlStr := "select c.*, f.*, p.*, i.* " +
		" from collections c " +
		" left join field_schemas f on c.collection_id = f.collection_id and c.ts = f.ts and f.is_deleted=false " +
		" left join partitions p on c.collection_id = p.collection_id and c.ts = p.ts and p.is_deleted=false " +
		" left join indexes i on c.collection_id = i.collection_id and i.is_deleted=false " +
		" where c.is_deleted=false and c.collection_id=? and c.ts=?"
	var result []struct {
		Collection
		Partition
		Field
		Index
	}
	err := tc.DB.Select(&result, sqlStr, collectionID, ts)
	if err != nil {
		log.Error("get collection by id failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	var colls []*model.Collection
	for _, record := range result {
		c := ConvertCollectionDBToModel(&record.Collection, &record.Partition, &record.Field, &record.Index)
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
	err := tc.DB.Get(&collID, sqlStr, collectionName, ts)
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
		" left join field_schemas f on c.collection_id = f.collection_id and c.ts = f.ts and f.is_deleted=false " +
		" left join partitions p on c.collection_id = p.collection_id and c.ts = p.ts and p.is_deleted=false " +
		" left join indexes i on c.collection_id = i.collection_id and i.is_deleted=false " +
		" where c.is_deleted=false and c.ts=?"
	var result []struct {
		Collection
		Partition
		Field
		Index
	}
	err := tc.DB.Select(&result, sqlStr, ts)
	if err != nil {
		log.Error("list collection failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	var colls []*model.Collection
	for _, record := range result {
		c := ConvertCollectionDBToModel(&record.Collection, &record.Partition, &record.Field, &record.Index)
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
	sqlStr4 := "update indexes set is_deleted=true where collection_id=?"
	rs, err = tx.Exec(sqlStr4, collectionInfo.CollectionID)
	if err != nil {
		log.Error("update indexes by collection ID failed", zap.Error(err))
		return err
	}
	n, err = rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table indexes RowsAffected:", zap.Any("rows", n))

	// sql 5
	sqlStr5 := "update segment_indexes set is_deleted=true where collection_id=?"
	rs, err = tx.Exec(sqlStr5, collectionInfo.CollectionID)
	if err != nil {
		log.Error("update segment_indexes by collection ID failed", zap.Error(err))
		return err
	}
	n, err = rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table segment_indexes RowsAffected:", zap.Any("rows", n))

	// sql 6
	if ddOpStr, ok := collectionInfo.Extra[metastore.DDOperationPrefix]; ok {
		sqlStr6 := "insert into dd_msg_send(operation_type, operation_body, is_sent, ts) values (?,?,?,?)"
		var ddOp metastore.DdOperation
		err = metastore.DecodeDdOperation(ddOpStr, &ddOp)
		if err != nil {
			log.Error("decode DD operation failed", zap.Error(err))
			return err
		}
		isDDMsgSent := "false"
		_, err = tx.Exec(sqlStr6, ddOp.Type, ddOpStr, isDDMsgSent, ts)
		if err != nil {
			log.Error("insert dd_msg_send failed", zap.Error(err))
			return err
		}
	}

	return err
}

func (tc *TableCatalog) CreatePartition(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	// sql 1
	sqlStr1 := "insert into partitions(partition_id, partition_name, partition_created_timestamp, collection_id, ts) values (:partition_id, :partition_name, :partition_created_timestamp, :collection_id, :ts)"
	var partitions []Partition
	for _, partition := range coll.Partitions {
		p := Partition{
			PartitionID:               &partition.PartitionID,
			PartitionName:             &partition.PartitionName,
			PartitionCreatedTimestamp: &partition.PartitionCreatedTimestamp,
			CollectionID:              &coll.CollectionID,
			Ts:                        ts,
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
		return errors.New("RowsAffected is not 1")
	}

	return nil
}

func (tc *TableCatalog) CreateIndex(ctx context.Context, index *model.SegmentIndex) error {
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
	sqlStr1 := "insert into indexes(collection_id, field_id, index_id, index_name, index_params) values (:collection_id, :field_id, :index_id, :index_name, :index_params)"
	indexParamsBytes, err := json.Marshal(index.IndexParams)
	if err != nil {
		log.Error("marshal IndexParams of field failed", zap.Error(err))
	}
	indexParamsStr := string(indexParamsBytes)
	idx := Index{
		FieldID:      &index.FieldID,
		CollectionID: &index.CollectionID,
		IndexID:      &index.IndexID,
		IndexName:    &index.IndexName,
		IndexParams:  &indexParamsStr,
	}
	_, err = tx.NamedExec(sqlStr1, idx)
	if err != nil {
		log.Error("insert indexes failed", zap.Error(err))
		return err
	}

	// sql 2
	sqlStr2 := "insert into segment_indexes(collection_id, partition_id, segment_id, field_id, index_id, build_id, enable_index, index_file_paths, index_size) values (:collection_id, :partition_id, :segment_id, :field_id, :index_id, :build_id, :enable_index, :index_file_paths, :index_size)"
	indexFilePaths, err := json.Marshal(index.IndexFilePaths)
	if err != nil {
		log.Error("marshal alias failed", zap.Error(err))
		return err
	}
	indexFilePathsStr := string(indexFilePaths)
	fi := SegmentIndex{
		CollectionID:   index.CollectionID,
		PartitionID:    index.PartitionID,
		SegmentID:      index.SegmentID,
		FieldID:        index.FieldID,
		IndexID:        index.IndexID,
		BuildID:        index.BuildID,
		EnableIndex:    index.EnableIndex,
		IndexFilePaths: indexFilePathsStr,
		IndexSize:      index.IndexSize,
	}
	_, err = tx.NamedExec(sqlStr2, fi)
	if err != nil {
		log.Error("insert segment_indexes failed", zap.Error(err))
		return err
	}

	return nil
}

func (tc *TableCatalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
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
	sqlStr1 := "update indexes set is_deleted=true where index_id=?"
	rs, err := tx.Exec(sqlStr1, dropIdxID)
	if err != nil {
		log.Error("update indexes by index ID failed", zap.Error(err), zap.Int64("indexID", dropIdxID))
		return err
	}

	// sql 2
	sqlStr2 := "update segment_indexes set is_deleted=true where index_id=?"
	rs, err = tx.Exec(sqlStr2, dropIdxID)
	if err != nil {
		log.Error("update segment_indexes by index ID failed", zap.Error(err), zap.Int64("indexID", dropIdxID))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table segment_indexes RowsAffected:", zap.Any("rows", n))

	return nil
}

func (tc *TableCatalog) ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error) {
	sqlStr := "select collection_id, partition_id, segment_id, field_id, index_id, build_id from segment_indexes where is_deleted=false"
	var indexes []*SegmentIndex
	err := tc.DB.Select(&indexes, sqlStr)
	if err != nil {
		log.Error("list segment indexes failed", zap.Error(err))
		return nil, err
	}
	return BatchConvertSegmentIndexDBToModel(indexes), nil
}

func (tc *TableCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	sqlStr := "select index_id, index_name, index_params from indexes where is_deleted=false"
	var indexes []*Index
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
		log.Error("get collection by ID failed", zap.Int64("collID", collection.CollectionID), zap.Uint64("ts", ts))
		return err
	}
	presentAlias := coll.Aliases
	newAlias := collection.Aliases
	aliasSlice := metastore.ConvertInterfaceSlice(typeutil.SliceRemoveDuplicate(append(presentAlias, newAlias...)))
	aliasesBytes, err := json.Marshal(aliasSlice)
	aliasesStr := string(aliasesBytes)
	if err != nil {
		log.Error("marshal alias failed", zap.Error(err))
		return err
	}
	// sql 1
	sqlStr1 := "update collections set collection_alias=? where collection_id=? and ts=?"
	rs, err := tc.DB.Exec(sqlStr1, aliasesStr, collection.CollectionID, ts)
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
	aliasesBytes, err := json.Marshal(presentAlias)
	aliasesStr := string(aliasesBytes)
	if err != nil {
		log.Error("marshal alias failed", zap.Error(err))
		return err
	}
	// sql 1
	sqlStr1 := "update collections set collection_alias=? where collection_id=? and ts=?"
	rs, err := tc.DB.Exec(sqlStr1, aliasesStr, collectionID, ts)
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
	sqlStr := "select collection_id, collection_alias from collections where is_deleted=false and collection_alias is not null"
	var colls []*Collection
	err := tc.DB.Select(&colls, sqlStr)
	if err != nil {
		log.Error("list collection alias failed", zap.Error(err))
		return nil, err
	}

	var collAlias []*model.Collection
	for _, coll := range colls {
		var aliases []string
		err = json.Unmarshal([]byte(*coll.CollectionAlias), aliases)
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
