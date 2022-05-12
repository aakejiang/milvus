package table

import (
	"encoding/json"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// model <---> db

func ConvertCollectionDBToModel(coll *Collection, partition *Partition, field *Field, index *IndexBuilder) *model.Collection {
	var aliases []string
	err := json.Unmarshal([]byte(coll.CollectionAlias), aliases)
	if err != nil {
		log.Error("unmarshal collection alias failed", zap.Error(err))
	}
	properties := CollProperties{}
	err = json.Unmarshal([]byte(coll.Properties), properties)
	if err != nil {
		log.Error("unmarshal collection properties error", zap.Error(err))
	}
	var retFields []*model.Field
	retFields = append(retFields, ConvertFieldDBToModel(field))
	var retPartitions []*model.Partition
	retPartitions = append(retPartitions, ConvertPartitionDBToModel(partition))
	var retIndexes []*model.Index
	retIndexes = append(retIndexes, ConvertIndexDBToModel(index))
	return &model.Collection{
		CollectionID:         coll.CollectionID,
		Name:                 coll.CollectionName,
		Description:          coll.Description,
		AutoID:               coll.AutoID,
		Fields:               retFields,
		Partitions:           retPartitions,
		FieldIndexes:         retIndexes,
		VirtualChannelNames:  properties.VirtualChannelNames,
		PhysicalChannelNames: properties.PhysicalChannelNames,
		ShardsNum:            properties.ShardsNum,
		StartPositions:       properties.StartPositions,
		ConsistencyLevel:     properties.ConsistencyLevel,
		CreateTime:           coll.CreatedTime,
		Aliases:              aliases,
	}
}

func ConvertCollectionsToIDMap(colls []*model.Collection) map[typeutil.UniqueID]*model.Collection {
	var colMap map[typeutil.UniqueID]*model.Collection
	for _, c := range colls {
		if existColl, ok := colMap[c.CollectionID]; !ok {
			colMap[c.CollectionID] = c
		} else {
			existColl.Fields = append(existColl.Fields, c.Fields...)
			existColl.Partitions = append(existColl.Partitions, c.Partitions...)
			existColl.FieldIndexes = append(existColl.FieldIndexes, c.FieldIndexes...)
		}
	}
	return colMap
}

func ConvertCollectionsToNameMap(colls []*model.Collection) map[string]*model.Collection {
	var colMap map[string]*model.Collection
	for _, c := range colls {
		if existColl, ok := colMap[c.Name]; !ok {
			colMap[c.Name] = c
		} else {
			existColl.Fields = append(existColl.Fields, c.Fields...)
			existColl.Partitions = append(existColl.Partitions, c.Partitions...)
			existColl.FieldIndexes = append(existColl.FieldIndexes, c.FieldIndexes...)
		}
	}
	return colMap
}

func ConvertFieldDBToModel(field *Field) *model.Field {
	return &model.Field{
		FieldID:      field.FieldID,
		Name:         field.FieldName,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  field.Description,
		DataType:     field.DataType,
		TypeParams:   field.TypeParams,
		IndexParams:  field.IndexParams,
		AutoID:       field.AutoID,
	}
}

func BatchConvertFieldDBToModel(fields []*Field) []*model.Field {
	var result []*model.Field
	for _, f := range fields {
		field := ConvertFieldDBToModel(f)
		result = append(result, field)
	}
	return result
}

func ConvertPartitionDBToModel(partiton *Partition) *model.Partition {
	return &model.Partition{
		PartitionID:               partiton.PartitionID,
		PartitionName:             partiton.PartitionName,
		PartitionCreatedTimestamp: partiton.PartitionCreatedTimestamp,
	}
}

func BatchConvertPartitionDBToModel(partitons []*Partition) []*model.Partition {
	var result []*model.Partition
	for _, p := range partitons {
		partition := ConvertPartitionDBToModel(p)
		result = append(result, partition)
	}
	return result
}

func ConvertIndexDBToModel(fieldIndex *IndexBuilder) *model.Index {
	return &model.Index{
		CollectionID: fieldIndex.CollectionID,
		PartitionID:  fieldIndex.PartitionID,
		SegmentID:    fieldIndex.SegmentID,
		FieldID:      fieldIndex.FieldID,
		IndexID:      fieldIndex.IndexID,
		BuildID:      fieldIndex.BuildID,
		//EnableIndex:  fieldIndex.EnableIndex, // TODO populate it!!!
	}
}

func BatchConvertIndexDBToModel(fieldIndexes []*IndexBuilder) []*model.Index {
	var indexes []*model.Index
	for _, fi := range fieldIndexes {
		index := ConvertIndexDBToModel(fi)
		indexes = append(indexes, index)
	}
	return indexes
}

func ConvertUserDBToModel(user *User) *model.Credential {
	return &model.Credential{
		Username:          user.Username,
		EncryptedPassword: user.EncryptedPassword,
	}
}
