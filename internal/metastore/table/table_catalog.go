package table

import (
	"context"
	"database/sql"
	"encoding/json"
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

const sqlJoin = `select
collections.*,
field_schemas.field_id, field_schemas.field_name, field_schemas.is_primary_key, field_schemas.description, field_schemas.data_type, field_schemas.type_params, field_schemas.auto_id,
partitions.partition_id, partitions.partition_name, partitions.partition_created_timestamp
from collections
left join field_schemas on collections.collection_id = field_schemas.collection_id and collections.ts = field_schemas.ts and field_schemas.is_deleted=false
left join partitions on collections.collection_id = partitions.collection_id and collections.ts = partitions.ts and partitions.is_deleted=false`

func (tc *TableCatalog) CreateCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "insert into collections(tenant_id, collection_id, collection_name, collection_alias, description, auto_id, ts, properties) values (?,?,?,?,?,?,?,?)"
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
				FieldID:      field.FieldID,
				FieldName:    field.Name,
				IsPrimaryKey: field.IsPrimaryKey,
				Description:  &field.Description,
				DataType:     field.DataType,
				TypeParams:   &typeParamsStr,
				IndexParams:  &indexParamsStr,
				AutoID:       field.AutoID,
				CollectionID: collection.CollectionID,
				Ts:           ts,
			}
			fields = append(fields, f)
		}
		if len(fields) != 0 {
			_, err = tx.NamedExec(sqlStr2, fields)
			if err != nil {
				log.Error("insert field_schemas failed", zap.Error(err))
				return err
			}
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
				Ts:                        ts,
			}
			partitions = append(partitions, p)
		}
		_, err = tx.NamedExec(sqlStr3, partitions)
		if err != nil {
			log.Error("insert partitions failed", zap.Error(err))
			return err
		}

		// sql 4
		if ddOpStr, ok := collection.Extra[metastore.DDOperationPrefix]; ok {
			var ddOp = ddOpStr.(model.DdOperation)
			if ddOp.Type != "" && ddOp.Body != "" {
				sqlStr4 := "insert into dd_msg_send(operation_type, operation_body, is_sent) values (?,?,?)"
				isDDMsgSent := false
				_, err = tx.Exec(sqlStr4, ddOp.Type, ddOp.Body, isDDMsgSent)
				if err != nil {
					log.Error("insert dd_msg_send failed", zap.Error(err))
					return err
				}
			}
		}

		return nil
	})
}

func (tc *TableCatalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	var result []struct {
		Collection
		Partition
		Field
	}
	sqlStr := sqlJoin + " where collections.collection_id=?"
	var err error
	if ts > 0 {
		sqlStr = sqlStr + " and collections.ts<=?"
		err = tc.DB.Unsafe().Select(&result, sqlStr, collectionID, ts)
	} else {
		sqlStr = sqlStr + " and collections.is_deleted=false"
		err = tc.DB.Unsafe().Select(&result, sqlStr, collectionID)
	}
	if err != nil {
		log.Error("get collection by id failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	indexMap, _ := tc.listIndexesByCollectionID(collectionID) // populate index info
	var colls []*model.Collection
	for _, record := range result {
		index, _ := indexMap[collectionID]
		c := ConvertCollectionDBToModel(&record.Collection, &record.Partition, &record.Field, &index)
		colls = append(colls, c)
	}
	collMap := ConvertCollectionsToIDMap(colls)
	if _, ok := collMap[collectionID]; !ok {
		log.Error("not found collection in the map", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, fmt.Errorf("not found collectionId %d in the map, ts = %d", collectionID, ts)
	}
	return collMap[collectionID], nil
}

func (tc *TableCatalog) GetCollectionIDByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	sqlStr := "select collection_id from collections where collection_name=?"
	var collID typeutil.UniqueID
	var err error
	if ts > 0 {
		sqlStr = sqlStr + " and ts<=? order by ts desc limit 1"
		err = tc.DB.Get(&collID, sqlStr, collectionName, ts)
	} else {
		sqlStr = sqlStr + " and is_deleted=false"
		err = tc.DB.Get(&collID, sqlStr, collectionName)
	}
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
	var result []struct {
		Collection
		Partition
		Field
	}
	sqlStr := sqlJoin
	var err error
	if ts > 0 {
		sqlStr = sqlStr + " where collections.ts<=?"
		err = tc.DB.Unsafe().Select(&result, sqlStr, ts)
	} else {
		sqlStr = sqlStr + " where collections.is_deleted=false"
		err = tc.DB.Unsafe().Select(&result, sqlStr)
	}
	if err != nil {
		log.Error("list collection failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	indexMap, _ := tc.listIndexesByCollectionID(-1) // populate index info
	var colls []*model.Collection
	for _, record := range result {
		coll := record.Collection
		index, _ := indexMap[coll.CollectionID]
		c := ConvertCollectionDBToModel(&coll, &record.Partition, &record.Field, &index)
		colls = append(colls, c)
	}
	return ConvertCollectionsToNameMap(colls), nil
}

func (tc *TableCatalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	sqlStr := "select tenant_id, collection_id, collection_name, description, auto_id, ts, properties, created_at from collections where collection_id=?"
	var coll Collection
	var err error
	if ts > 0 {
		sqlStr = sqlStr + " and ts<=?"
		err = tc.DB.Get(&coll, sqlStr, collectionID, ts)
	} else {
		sqlStr = sqlStr + " and is_deleted=false"
		err = tc.DB.Get(&coll, sqlStr, collectionID)
	}
	if err != nil {
		log.Error("get collection by ID failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		// Also, an error is returned if the result set is empty.
		return false
	}
	return true
}

func (tc *TableCatalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	return WithTransaction(tc.DB, func(tx Transaction) error {
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
		log.Debug("table collections RowsAffected", zap.Any("rows", n))

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
		log.Debug("table partitions RowsAffected", zap.Any("rows", n))

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
		log.Debug("table field_schemas RowsAffected", zap.Any("rows", n))

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
		log.Debug("table indexes RowsAffected", zap.Any("rows", n))

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
		log.Debug("table segment_indexes RowsAffected", zap.Any("rows", n))

		// sql 6
		if ddOpStr, ok := collectionInfo.Extra[metastore.DDOperationPrefix]; ok {
			var ddOp = ddOpStr.(model.DdOperation)
			if ddOp.Type != "" && ddOp.Body != "" {
				sqlStr6 := "insert into dd_msg_send(operation_type, operation_body, is_sent) values (?,?,?)"
				isDDMsgSent := false
				rs, err = tx.Exec(sqlStr6, ddOp.Type, ddOp.Body, isDDMsgSent)
				if err != nil {
					log.Error("insert dd_msg_send failed", zap.Error(err))
					return err
				}
				n, err = rs.RowsAffected()
				if err != nil {
					log.Error("get RowsAffected failed", zap.Error(err))
					return err
				}
				log.Debug("table dd_msg_send RowsAffected", zap.Any("rows", n))
			}
		}

		return err
	})
}

func (tc *TableCatalog) CreatePartition(ctx context.Context, coll *model.Collection, partition *model.Partition, ts typeutil.Timestamp) error {
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "insert into partitions(partition_id, partition_name, partition_created_timestamp, collection_id, ts) values (:partition_id, :partition_name, :partition_created_timestamp, :collection_id, :ts)"
		p := Partition{
			PartitionID:               partition.PartitionID,
			PartitionName:             partition.PartitionName,
			PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
			CollectionID:              coll.CollectionID,
			Ts:                        ts,
		}
		_, err := tc.DB.NamedExec(sqlStr1, []Partition{p})
		if err != nil {
			log.Error("insert partitions failed", zap.Error(err))
			return err
		}

		// sql 2
		if ddOpStr, ok := coll.Extra[metastore.DDOperationPrefix]; ok {
			var ddOp = ddOpStr.(model.DdOperation)
			if ddOp.Type != "" && ddOp.Body != "" {
				sqlStr2 := "insert into dd_msg_send(operation_type, operation_body, is_sent) values (?,?,?)"
				isDDMsgSent := false
				_, err = tx.Exec(sqlStr2, ddOp.Type, ddOp.Body, isDDMsgSent)
				if err != nil {
					log.Error("insert dd_msg_send failed", zap.Error(err))
					return err
				}
			}
		}

		return nil
	})
}

func (tc *TableCatalog) DropPartition(ctx context.Context, collection *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	return WithTransaction(tc.DB, func(tx Transaction) error {
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
		log.Debug("table partitions RowsAffected", zap.Any("rows", n))

		// sql 2
		if ddOpStr, ok := collection.Extra[metastore.DDOperationPrefix]; ok {
			var ddOp = ddOpStr.(model.DdOperation)
			if ddOp.Type != "" && ddOp.Body != "" {
				sqlStr2 := "insert into dd_msg_send(operation_type, operation_body, is_sent) values (?,?,?)"
				isDDMsgSent := false
				_, err = tx.Exec(sqlStr2, ddOp.Type, ddOp.Body, isDDMsgSent)
				if err != nil {
					log.Error("insert dd_msg_send failed", zap.Error(err))
					return err
				}
			}
		}

		return nil
	})
}

func (tc *TableCatalog) CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error {
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "insert into indexes(collection_id, field_id, index_id, index_name, index_params) values (:collection_id, :field_id, :index_id, :index_name, :index_params)"
		indexParamsBytes, err := json.Marshal(index.IndexParams)
		if err != nil {
			log.Error("marshal IndexParams of field failed", zap.Error(err))
		}
		indexParamsStr := string(indexParamsBytes)
		idx := Index{
			FieldID:      index.FieldID,
			CollectionID: index.CollectionID,
			IndexID:      index.IndexID,
			IndexName:    index.IndexName,
			IndexParams:  indexParamsStr,
		}
		_, err = tx.NamedExec(sqlStr1, idx)
		if err != nil {
			log.Error("insert indexes failed", zap.Error(err))
			return err
		}

		// sql 2
		sqlStr2 := "insert into segment_indexes(collection_id, partition_id, segment_id, field_id, index_id, build_id, enable_index, index_file_paths, index_size) values (:collection_id, :partition_id, :segment_id, :field_id, :index_id, :build_id, :enable_index, :index_file_paths, :index_size)"
		var segIndexes []SegmentIndex
		for _, segIndex := range index.SegmentIndexes {
			indexFilePaths, err := json.Marshal(segIndex.IndexFilePaths)
			if err != nil {
				log.Error("marshal alias failed", zap.Error(err))
				continue
			}
			indexFilePathsStr := string(indexFilePaths)
			si := SegmentIndex{
				CollectionID:   index.CollectionID,
				PartitionID:    segIndex.PartitionID,
				SegmentID:      segIndex.SegmentID,
				FieldID:        index.FieldID,
				IndexID:        index.IndexID,
				BuildID:        segIndex.BuildID,
				EnableIndex:    segIndex.EnableIndex,
				IndexFilePaths: indexFilePathsStr,
				IndexSize:      segIndex.IndexSize,
			}
			segIndexes = append(segIndexes, si)
		}
		_, err = tx.NamedExec(sqlStr2, segIndexes)
		if err != nil {
			log.Error("insert segment_indexes failed", zap.Error(err))
			return err
		}

		return nil
	})
}

func (tc *TableCatalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index) error {
	return nil
}

func (tc *TableCatalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "update indexes set is_deleted=true where index_id=?"
		rs, err := tx.Exec(sqlStr1, dropIdxID)
		if err != nil {
			log.Error("update indexes by index ID failed", zap.Error(err), zap.Int64("indexID", dropIdxID))
			return err
		}
		n, err := rs.RowsAffected()
		if err != nil {
			log.Error("get RowsAffected failed", zap.Error(err))
			return err
		}
		log.Debug("table indexes RowsAffected", zap.Any("rows", n))

		// sql 2
		sqlStr2 := "update segment_indexes set is_deleted=true where index_id=?"
		rs, err = tx.Exec(sqlStr2, dropIdxID)
		if err != nil {
			log.Error("update segment_indexes by index ID failed", zap.Error(err), zap.Int64("indexID", dropIdxID))
			return err
		}
		n, err = rs.RowsAffected()
		if err != nil {
			log.Error("get RowsAffected failed", zap.Error(err))
			return err
		}
		log.Debug("table segment_indexes RowsAffected", zap.Any("rows", n))

		return nil
	})
}

func (tc *TableCatalog) listIndexesByCollectionID(collID typeutil.UniqueID) (map[int64]Index, error) {
	var resultMap map[int64]Index

	var indexes []Index
	sqlStr := `select * from indexes where is_deleted=false`
	args := []interface{}{}
	if collID > 0 {
		sqlStr = sqlStr + " and collection_id=?"
		args = append(args, collID)
	}
	//if ts > 0 {
	//	sqlStr = sqlStr + " and ts<=?"
	//	args = append(args, ts)
	//}
	err := tc.DB.Select(&indexes, sqlStr, args...)
	if err != nil {
		log.Error("list indexes by collectionID failed", zap.Int64("collID", collID), zap.Error(err))
		return resultMap, err
	}

	resultMap = make(map[int64]Index)
	for _, idx := range indexes {
		resultMap[idx.CollectionID] = idx
	}

	return resultMap, nil
}

func (tc *TableCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	sqlStr := `select
		segment_indexes.*,
    	indexes.field_id, indexes.collection_id, indexes.index_id, indexes.index_name, indexes.index_params
		from indexes
		left join segment_indexes on indexes.index_id = segment_indexes.index_id and segment_indexes.is_deleted = false
		where indexes.is_deleted=false`
	var result []struct {
		Index
		SegmentIndex
	}
	err := tc.DB.Unsafe().Select(&result, sqlStr)
	if err != nil {
		log.Error("list indexes failed", zap.Error(err))
		return nil, err
	}

	indexMap := ConvertIndexesToMap(result)
	indexes := make([]*model.Index, 0, len(indexMap))
	for _, idx := range indexMap {
		indexes = append(indexes, idx)
	}
	return indexes, nil
}

func (tc *TableCatalog) AddAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
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
	log.Debug("table collections RowsAffected", zap.Any("rows", n))

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
	log.Debug("table collections RowsAffected", zap.Any("rows", n))

	return nil
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
		if coll.CollectionAlias == nil {
			log.Warn("collection alias is nil")
			continue
		}
		err = json.Unmarshal([]byte(*coll.CollectionAlias), &aliases)
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
	var user User
	err := tc.DB.Get(&user, sqlStr, username)
	if err != nil {
		log.Error("get credential user by username failed", zap.Error(err))
		return nil, err
	}
	return ConvertUserDBToModel(&user), nil
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
	log.Debug("table credential_users RowsAffected", zap.Any("rows", n))
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

func (tc *TableCatalog) IsDDMsgSent(ctx context.Context) (bool, error) {
	ddOp, err := tc.LoadDdOperation(ctx)
	if ddOp == (model.DdOperation{}) || err != nil {
		return true, err
	}
	return ddOp.IsSent, nil
}

func (tc *TableCatalog) LoadDdOperation(ctx context.Context) (model.DdOperation, error) {
	var ddOp DdOperation
	sqlStr := "select * from dd_msg_send order by updated_at desc limit 1"
	err := tc.DB.Get(&ddOp, sqlStr)
	if err != nil && err != sql.ErrNoRows { // err.Error() != "sql: no rows in result set"
		log.Error("get dd-operation failed", zap.Error(err))
		return model.DdOperation{}, err
	}

	return ConvertDdOperationDBToModel(ddOp), nil
}

func (tc *TableCatalog) UpdateDDOperation(ctx context.Context, ddOp model.DdOperation, isSent bool) error {
	sql := "update dd_msg_send set is_sent=? where operation_type=? and operation_body=?"
	_, err := tc.DB.Exec(sql, isSent, ddOp.Type, ddOp.Body)
	if err != nil {
		log.Error("update dd_msg_send failed", zap.Error(err))
		return err
	}
	return nil
}

func (tc *TableCatalog) Close() {

}
