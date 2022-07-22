package table

import (
	"context"
	"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/contextutil"
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
left join partitions on collections.collection_id = partitions.collection_id and collections.ts = partitions.ts and partitions.is_deleted=false
where collections.is_deleted=false`

func (tc *TableCatalog) CreateCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "insert into collections(tenant_id, collection_id, collection_name, description, auto_id, ts, properties) values (?,?,?,?,?,?,?)"
		properties := ConvertToCollectionProperties(collection)
		propertiesBytes, err := json.Marshal(properties)
		propertiesStr := string(propertiesBytes)
		if err != nil {
			log.Error("marshal collection properties error", zap.Error(err))
			return err
		}
		_, err = tx.Exec(sqlStr1, tenantID, collection.CollectionID, collection.Name, collection.Description, collection.AutoID, ts, propertiesStr)
		if err != nil {
			log.Error("insert collection failed", zap.Error(err))
			return err
		}

		// sql 2
		sqlStr2 := "insert into field_schemas(tenant_id, field_id, field_name, is_primary_key, description, data_type, type_params, index_params, auto_id, collection_id, ts) values (:tenant_id, :field_id, :field_name, :is_primary_key, :description, :data_type, :type_params, :index_params, :auto_id, :collection_id, :ts)"
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
				TenantID:     &collection.TenantID,
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
		sqlStr3 := "insert into partitions(tenant_id, partition_id, partition_name, partition_created_timestamp, collection_id, ts) values (:tenant_id, :partition_id, :partition_name, :partition_created_timestamp, :collection_id, :ts)"
		var partitions []Partition
		for _, partition := range collection.Partitions {
			p := Partition{
				TenantID:                  &collection.TenantID,
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

		return nil
	})
}

func (tc *TableCatalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	// For time travel (ts > 0), queryTs is the largest timestamp that less than or equal to @param ts (queryTs <= ts)
	// otherwise if ts = 0, queryTs is also 0
	var queryTs typeutil.Timestamp
	if ts > 0 {
		sqlStr1 := "select ts, is_deleted from collections where collection_id=? and ts>0 and ts<=? order by ts desc limit 1"
		var tsDeletedPair struct {
			Ts        typeutil.Timestamp `db:"ts"`
			IsDeleted bool               `db:"is_deleted"`
		}
		err := tc.DB.Get(&tsDeletedPair, sqlStr1, collectionID, ts)
		if err != nil {
			log.Error("get collection ts failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
			return nil, err
		}
		if tsDeletedPair.IsDeleted {
			return &model.Collection{}, nil
		}
		queryTs = tsDeletedPair.Ts
	} else {
		queryTs = 0
	}

	var result []struct {
		Collection
		Partition
		Field
	}
	sqlStr := sqlJoin + " and collections.collection_id=? and collections.ts=?"
	args := []interface{}{collectionID, queryTs}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and collections.tenant_id=?"
		args = append(args, tenantID)
	}
	err := tc.DB.Unsafe().Select(&result, sqlStr, args...)
	if err != nil {
		log.Error("get collection by id failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	indexMap, _ := tc.listIndexesByCollectionID(ctx, collectionID) // populate index info
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

// GetCollectionIDByName since collection name will not change for different ts
func (tc *TableCatalog) GetCollectionIDByName(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	sqlStr := "select collection_id from collections where collection_name=? limit 1"
	args := []interface{}{collectionName}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and tenant_id=?"
		args = append(args, tenantID)
	}

	var collID typeutil.UniqueID
	err := tc.DB.Get(&collID, sqlStr, args...)
	if err != nil {
		log.Error("get collection id by name failed", zap.String("collName", collectionName), zap.Error(err))
		return 0, err
	}

	return collID, nil
}

func (tc *TableCatalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	// Since collection name will not change for different ts
	collectionID, err := tc.GetCollectionIDByName(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	return tc.GetCollectionByID(ctx, collectionID, ts)
}

// ListCollections For time travel (ts > 0), find only one record respectively for each collection no matter `is_deleted` is true or false
// i.e. there are 3 collections in total,
// [collection1, t1, is_deleted=true]
// [collection2, t2, is_deleted=false]
// [collection3, t3, is_deleted=false]
// t1, t2, t3 are the largest timestamp that less than or equal to @param ts
// the final result will only return collection2 and collection3 since collection1 is deleted
func (tc *TableCatalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	// 1. find each collection with latest ts
	sqlStr1 := "select collection_id, max(ts) ts from collections where ts<=? group by collection_id"
	var inPairs []struct {
		CollectionID string             `db:"collection_id"`
		Ts           typeutil.Timestamp `db:"ts"`
	}
	err := tc.DB.Select(&inPairs, sqlStr1, ts)
	if err != nil {
		log.Error("list latest ts and corresponding collectionID in collections failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}
	if len(inPairs) == 0 {
		return map[string]*model.Collection{}, nil
	}

	// 2. select with in clause
	inPlaceHolders := sqlInPlaceholders(len(inPairs))
	sqlStr2 := fmt.Sprintf(sqlJoin+" and (collections.collection_id, collections.ts) in (%s)", inPlaceHolders)

	args := []interface{}{}
	for _, pair := range inPairs {
		args = append(args, []interface{}{pair.CollectionID, pair.Ts})
	}

	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr2 = sqlStr2 + " and collections.tenant_id=?"
		args = append(args, tenantID)
	}
	query, newArgs, err := sqlx.In(sqlStr2, args...)
	if err != nil {
		log.Error("sql in clause error", zap.Error(err))
		return nil, err
	}
	query = tc.DB.Rebind(query) // Rebind query
	var result []struct {
		Collection
		Partition
		Field
	}
	err = tc.DB.Unsafe().Select(&result, query, newArgs...)
	if err != nil {
		log.Error("list collection failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	// 3. populate index info
	indexMap, _ := tc.listIndexesByCollectionID(ctx, -1)
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
	sqlStr := "select tenant_id, collection_id, ts, is_deleted from collections where collection_id=?"
	args := []interface{}{collectionID}
	if ts > 0 {
		sqlStr = sqlStr + " and ts>0 and ts<=? order by ts desc limit 1"
		args = append(args, ts)
	} else {
		sqlStr = sqlStr + " and ts=0"
	}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and tenant_id=?"
		args = append(args, tenantID)
	}

	var coll Collection
	err := tc.DB.Get(&coll, sqlStr, args...)
	if err != nil {
		log.Error("get collection by ID failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		// Also, an error is returned if the result set is empty.
		return false
	}

	return !coll.IsDeleted
}

func (tc *TableCatalog) DropCollection(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)
	return WithTransaction(tc.DB, func(tx Transaction) error {
		if ts > 0 {
			// sql 1
			sqlStr1 := "insert into collections(tenant_id, collection_id, ts, is_deleted) values (?,?,?,?)"
			_, err := tx.Exec(sqlStr1, tenantID, collection.CollectionID, ts, true)
			if err != nil {
				log.Error("insert tombstone collection failed", zap.Error(err))
				return err
			}

			// sql 2
			var collAliases []CollectionAlias
			for _, alias := range collection.Aliases {
				collAliases = append(collAliases, CollectionAlias{
					TenantID:        &tenantID,
					CollectionID:    collection.CollectionID,
					CollectionAlias: alias,
					Ts:              ts,
					IsDeleted:       true,
				})
			}
			if len(collAliases) > 0 {
				sqlStr2 := "insert into collection_aliases(tenant_id, collection_id, collection_alias, ts, is_deleted) values (:tenant_id, :collection_id, :collection_alias, :ts, :is_deleted)"
				_, err = tx.NamedExec(sqlStr2, collAliases)
				if err != nil {
					log.Error("insert tombstone coll alias failed", zap.Error(err))
					return err
				}
			}
		} else {
			// sql 1
			sqlStr1 := "update collections set is_deleted=true where collection_id=? and ts=0"
			args1 := []interface{}{collection.CollectionID}
			if tenantID != "" {
				sqlStr1 = sqlStr1 + " and tenant_id=?"
				args1 = append(args1, tenantID)
			}
			rs, err := tx.Exec(sqlStr1, args1...)
			if err != nil {
				log.Error("update collection failed", zap.Error(err))
				return err
			}
			n, err := rs.RowsAffected()
			if err != nil {
				log.Error("get RowsAffected failed", zap.Error(err))
				return err
			}
			log.Debug("table collections RowsAffected", zap.Any("rows", n))

			// sql 2
			if len(collection.Aliases) > 0 {
				sqlStr2 := "update collection_aliases set is_deleted=true where collection_id=? and collection_alias in (?) and ts=0"
				args2 := []interface{}{collection.CollectionID, collection.Aliases}
				if tenantID != "" {
					sqlStr2 = sqlStr2 + " and tenant_id=?"
					args2 = append(args2, tenantID)
				}
				query, newArgs, err := sqlx.In(sqlStr2, args2...)
				if err != nil {
					log.Error("sql in clause error", zap.Error(err))
					return err
				}
				rs, err = tx.Exec(query, newArgs...)
				if err != nil {
					log.Error("update collection_aliases failed", zap.Error(err))
					return err
				}
				n, err = rs.RowsAffected()
				if err != nil {
					log.Error("get RowsAffected failed", zap.Error(err))
					return err
				}
				log.Debug("table collection_aliases RowsAffected", zap.Any("rows", n))
			}
		}

		// sql 3
		sqlStr3 := "update indexes set is_deleted=true where collection_id=?"
		args3 := []interface{}{collection.CollectionID}
		if tenantID != "" {
			sqlStr3 = sqlStr3 + " and tenant_id=?"
			args3 = append(args3, tenantID)
		}
		rs, err := tx.Exec(sqlStr3, args3...)
		if err != nil {
			log.Error("update indexes by collection ID failed", zap.Error(err))
			return err
		}
		n, err := rs.RowsAffected()
		if err != nil {
			log.Error("get RowsAffected failed", zap.Error(err))
			return err
		}
		log.Debug("table indexes RowsAffected", zap.Any("rows", n))

		// sql 4
		sqlStr4 := "update segment_indexes set is_deleted=true where collection_id=?"
		args4 := []interface{}{collection.CollectionID}
		if tenantID != "" {
			sqlStr4 = sqlStr4 + " and tenant_id=?"
			args4 = append(args4, tenantID)
		}
		rs, err = tx.Exec(sqlStr4, args4...)
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

		return err
	})
}

func (tc *TableCatalog) CreatePartition(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)
	// sql 1
	sqlStr1 := "insert into partitions(tenant_id, partition_id, partition_name, partition_created_timestamp, collection_id, ts) values (:tenant_id, :partition_id, :partition_name, :partition_created_timestamp, :collection_id, :ts)"
	partition := coll.Partitions[0]
	p := Partition{
		TenantID:                  &tenantID,
		PartitionID:               partition.PartitionID,
		PartitionName:             partition.PartitionName,
		PartitionCreatedTimestamp: partition.PartitionCreatedTimestamp,
		CollectionID:              coll.CollectionID,
		Ts:                        ts,
	}
	_, err := tc.DB.NamedExec(sqlStr1, p)
	if err != nil {
		log.Error("insert partitions failed", zap.Error(err))
		return err
	}

	return nil
}

func (tc *TableCatalog) DropPartition(ctx context.Context, collection *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)
	if ts > 0 {
		sqlStr1 := "insert into partitions(tenant_id, partition_id, partition_created_timestamp, collection_id, ts, is_deleted) values (?,?,?,?,?,?)"
		_, err := tc.DB.Exec(sqlStr1, tenantID, partitionID, collection.CollectionID, ts, ts, true)
		if err != nil {
			log.Error("insert tombstone partition failed", zap.Error(err))
			return err
		}
	} else {
		sqlStr1 := "update partitions set is_deleted=true where partition_id=? and ts=0"
		args := []interface{}{partitionID}
		if tenantID != "" {
			sqlStr1 = sqlStr1 + " and tenant_id=?"
			args = append(args, tenantID)
		}
		rs, err := tc.DB.Exec(sqlStr1, args...)
		if err != nil {
			log.Error("update partition failed", zap.Error(err))
			return err
		}
		n, err := rs.RowsAffected()
		if err != nil {
			log.Error("get RowsAffected failed", zap.Error(err))
			return err
		}
		log.Debug("table partitions RowsAffected", zap.Any("rows", n))
	}

	return nil
}

func (tc *TableCatalog) CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error {
	tenantID := contextutil.TenantID(ctx)
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "insert into indexes(tenant_id, collection_id, field_id, index_id, index_name, index_params) values (:tenant_id, :collection_id, :field_id, :index_id, :index_name, :index_params)"
		indexParamsBytes, err := json.Marshal(index.IndexParams)
		if err != nil {
			log.Error("marshal IndexParams of field failed", zap.Error(err))
		}
		indexParamsStr := string(indexParamsBytes)
		idx := Index{
			TenantID:     &tenantID,
			CollectionID: index.CollectionID,
			FieldID:      index.FieldID,
			IndexID:      index.IndexID,
			IndexName:    index.IndexName,
			IndexParams:  indexParamsStr,
		}
		rs, err := tx.NamedExec(sqlStr1, idx)
		if err != nil {
			log.Error("insert indexes failed", zap.Error(err))
			return err
		}
		n, err := rs.RowsAffected()
		if err != nil {
			log.Error("get RowsAffected failed", zap.Error(err))
			return err
		}
		log.Debug("table indexes RowsAffected", zap.Any("rows", n))

		// sql 2
		sqlStr2 := "insert into segment_indexes(tenant_id, collection_id, partition_id, segment_id, field_id, index_id, build_id, enable_index, index_file_paths, index_size) values (:tenant_id, :collection_id, :partition_id, :segment_id, :field_id, :index_id, :build_id, :enable_index, :index_file_paths, :index_size)"
		var segIndexes []SegmentIndex
		for _, segIndex := range index.SegmentIndexes {
			indexFilePaths, err := json.Marshal(segIndex.IndexFilePaths)
			if err != nil {
				log.Error("marshal IndexFilePaths failed", zap.Error(err))
				continue
			}
			indexFilePathsStr := string(indexFilePaths)
			si := SegmentIndex{
				TenantID:       &tenantID,
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
		if len(segIndexes) > 0 {
			rs, err := tx.NamedExec(sqlStr2, segIndexes)
			if err != nil {
				log.Error("insert segment_indexes failed", zap.Error(err))
				return err
			}
			n, err := rs.RowsAffected()
			if err != nil {
				log.Error("get RowsAffected failed", zap.Error(err))
				return err
			}
			log.Debug("table segment_indexes RowsAffected", zap.Any("rows", n))
		}

		return nil
	})
}

func (tc *TableCatalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index, alterType metastore.AlterType) error {
	// no need updating table indexes
	sqlStr := "update segment_indexes set partition_id=?, build_id=?, enable_index=?, index_file_paths=?, index_size=? where collection_id=? and segment_id=? and field_id=? and index_id=?"
	tenantID := contextutil.TenantID(ctx)
	for _, segIndex := range newIndex.SegmentIndexes {
		indexFilePaths, err := json.Marshal(segIndex.IndexFilePaths)
		if err != nil {
			log.Error("marshal alias failed", zap.Error(err))
			continue
		}
		indexFilePathsStr := string(indexFilePaths)
		args := []interface{}{segIndex.PartitionID, segIndex.BuildID, segIndex.EnableIndex, indexFilePathsStr, segIndex.IndexSize, oldIndex.CollectionID, segIndex.SegmentID, oldIndex.FieldID, oldIndex.IndexID}
		if tenantID != "" {
			sqlStr = sqlStr + " and tenant_id=?"
			args = append(args, tenantID)
		}
		_, err = tc.DB.Exec(sqlStr, args...)
		if err != nil {
			log.Error("update segment_indexes failed", zap.Error(err))
			return err
		}
	}

	return nil
}

func (tc *TableCatalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)
	return WithTransaction(tc.DB, func(tx Transaction) error {
		// sql 1
		sqlStr1 := "update indexes set is_deleted=true where index_id=?"
		args1 := []interface{}{dropIdxID}
		if tenantID != "" {
			sqlStr1 = sqlStr1 + " and tenant_id=?"
			args1 = append(args1, tenantID)
		}
		rs, err := tx.Exec(sqlStr1, args1...)
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
		args2 := []interface{}{dropIdxID}
		if tenantID != "" {
			sqlStr2 = sqlStr2 + " and tenant_id=?"
			args2 = append(args2, tenantID)
		}
		rs, err = tx.Exec(sqlStr2, args2...)
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

func (tc *TableCatalog) listIndexesByCollectionID(ctx context.Context, collID typeutil.UniqueID) (map[int64]Index, error) {
	var resultMap map[int64]Index

	sqlStr := `select * from indexes where is_deleted=false`
	args := []interface{}{}
	if collID > 0 {
		sqlStr = sqlStr + " and collection_id=?"
		args = append(args, collID)
	}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and tenant_id=?"
		args = append(args, tenantID)
	}
	var indexes []Index
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
	args := []interface{}{}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and indexes.tenant_id=?"
		args = append(args, tenantID)
	}
	var result []struct {
		Index
		SegmentIndex
	}
	err := tc.DB.Unsafe().Select(&result, sqlStr, args...)
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

func (tc *TableCatalog) CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	sqlStr := "insert into collection_aliases(tenant_id, collection_id, collection_alias, ts) values (:tenant_id, :collection_id, :collection_alias, :ts)"
	tenantID := contextutil.TenantID(ctx)
	collAlias := CollectionAlias{
		TenantID:        &tenantID,
		CollectionID:    collection.CollectionID,
		CollectionAlias: collection.Aliases[0],
		Ts:              ts,
	}
	rs, err := tc.DB.NamedExec(sqlStr, collAlias)
	if err != nil {
		log.Error("insert collection alias failed", zap.Error(err))
		return err
	}
	n, err := rs.RowsAffected()
	if err != nil {
		log.Error("get RowsAffected failed", zap.Error(err))
		return err
	}
	log.Debug("table collection_aliases RowsAffected", zap.Any("rows", n))

	return nil
}

func (tc *TableCatalog) DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	tenantID := contextutil.TenantID(ctx)
	if ts > 0 {
		sqlStr1 := "insert into collection_aliases(tenant_id, collection_id, collection_alias, ts, is_deleted) values (?,?,?,?,?)"
		_, err := tc.DB.Exec(sqlStr1, tenantID, collectionID, alias, ts, true)
		if err != nil {
			log.Error("insert tombstone coll alias failed", zap.Error(err))
			return err
		}
	} else {
		sqlStr := "update collection_aliases set is_deleted=true where collection_id=? and collection_alias=? and ts=0"
		args := []interface{}{collectionID, alias}
		if tenantID != "" {
			sqlStr = sqlStr + " and tenant_id=?"
			args = append(args, tenantID)
		}
		rs, err := tc.DB.Exec(sqlStr, args...)
		if err != nil {
			log.Error("drop coll alias failed", zap.Error(err))
			return err
		}
		n, err := rs.RowsAffected()
		if err != nil {
			log.Error("get RowsAffected failed", zap.Error(err))
			return err
		}
		log.Debug("table collection_aliases RowsAffected", zap.Any("rows", n))
	}

	return nil
}

func (tc *TableCatalog) AlterAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return nil
}

func sqlInPlaceholders(len int) string {
	ins := ""
	for i := 1; i <= len; i++ {
		if i != len {
			ins = ins + "(?),"
		} else {
			ins = ins + "(?)"
		}
	}
	return ins
}

// ListAliases query collection ID and aliases only, other information are not needed
func (tc *TableCatalog) ListAliases(ctx context.Context, ts typeutil.Timestamp) ([]*model.Collection, error) {
	// 1. find each collection with latest ts
	sqlStr1 := "select collection_id, max(ts) ts from collection_aliases where ts<=? group by collection_id"
	var inPairs []struct {
		CollectionID string             `db:"collection_id"`
		Ts           typeutil.Timestamp `db:"ts"`
	}
	err := tc.DB.Select(&inPairs, sqlStr1, ts)
	if err != nil {
		log.Error("list latest ts and corresponding collectionID in collection_aliases failed", zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}
	if len(inPairs) == 0 {
		return []*model.Collection{}, nil
	}

	// 2. select with in clause
	inPlaceHolders := sqlInPlaceholders(len(inPairs))
	sqlStr2 := fmt.Sprintf("select collection_id, collection_alias from collection_aliases where is_deleted=false and (collection_id, ts) in (%s)", inPlaceHolders)

	args := []interface{}{}
	for _, pair := range inPairs {
		args = append(args, []interface{}{pair.CollectionID, pair.Ts})
	}

	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr2 = sqlStr2 + " and tenant_id=?"
		args = append(args, tenantID)
	}

	query, newArgs, err := sqlx.In(sqlStr2, args...)
	if err != nil {
		log.Error("sql in clause error", zap.Error(err))
		return nil, err
	}
	//query = tc.DB.Rebind(query)
	var collAliases []CollectionAlias
	err = tc.DB.Select(&collAliases, query, newArgs...)
	if err != nil {
		log.Error("list collection alias failed", zap.Error(err))
		return nil, err
	}

	var colls []*model.Collection
	for _, record := range collAliases {
		colls = append(colls, &model.Collection{
			CollectionID: record.CollectionID,
			Name:         record.CollectionAlias,
		})
	}

	return colls, nil
}

func (tc *TableCatalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	sqlStr := "select tenant_id, username, encrypted_password, is_super, is_deleted from credential_users where is_deleted=false and username=?"
	args := []interface{}{username}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and tenant_id=?"
		args = append(args, tenantID)
	}
	var user User
	err := tc.DB.Get(&user, sqlStr, args...)
	if err != nil {
		log.Error("get credential user by username failed", zap.Error(err))
		return nil, err
	}
	return ConvertUserDBToModel(&user), nil
}

func (tc *TableCatalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	sqlStr1 := "insert into credential_users(tenant_id, username, encrypted_password) values (:tenant_id, :username, :encrypted_password)"
	tenantID := contextutil.TenantID(ctx)
	user := User{
		TenantID:          &tenantID,
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
	args := []interface{}{username}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr1 = sqlStr1 + " and tenant_id=?"
		args = append(args, tenantID)
	}
	rs, err := tc.DB.Exec(sqlStr1, args...)
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
	args := []interface{}{}
	tenantID := contextutil.TenantID(ctx)
	if tenantID != "" {
		sqlStr = sqlStr + " and tenant_id=?"
		args = append(args, tenantID)
	}
	var usernames []string
	err := tc.DB.Select(&usernames, sqlStr, args...)
	if err != nil {
		log.Error("list credential usernames failed", zap.Error(err))
		return nil, err
	}
	return usernames, nil
}

func (tc *TableCatalog) Close() {

}
