package table

import (
	"encoding/json"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

// model <---> db

func ConvertCollectionDBToModel(coll *Collection, partition *Partition, field *Field, index *SegmentIndex) *model.Collection {
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
	retIndexes = append(retIndexes, ConvertSegmentIndexDBToIndexModel(index))
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
		CreateTime:           coll.CreatedAt,
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
	var typeParams []*commonpb.KeyValuePair
	err := json.Unmarshal([]byte(field.TypeParams), typeParams)
	if err != nil {
		log.Error("unmarshal TypeParams of field failed", zap.Error(err))
	}
	var indexParams []*commonpb.KeyValuePair
	err = json.Unmarshal([]byte(field.IndexParams), indexParams)
	if err != nil {
		log.Error("unmarshal IndexParams of field failed", zap.Error(err))
	}
	return &model.Field{
		FieldID:      field.FieldID,
		Name:         field.FieldName,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  field.Description,
		DataType:     field.DataType,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
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

func ConvertIndexDBToModel(index *Index) *model.Index {
	return &model.Index{
		CollectionID: index.CollectionID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		IndexName:    index.IndexName,
		IndexParams:  index.IndexParams,
	}
}

func BatchConvertIndexDBToModel(indexes []*Index) []*model.Index {
	var result []*model.Index
	for _, i := range indexes {
		index := ConvertIndexDBToModel(i)
		result = append(result, index)
	}
	return result
}

func ConvertSegmentIndexDBToIndexModel(fieldIndex *SegmentIndex) *model.Index {
	return &model.Index{
		CollectionID: fieldIndex.CollectionID,
		FieldID:      fieldIndex.FieldID,
		IndexID:      fieldIndex.IndexID,
		IndexName:    fieldIndex.IndexName,
		IndexParams:  fieldIndex.IndexParams,
	}
}

func ConvertSegmentIndexDBToModel(fieldIndex *SegmentIndex) *model.SegmentIndex {
	return &model.SegmentIndex{
		Index: model.Index{
			CollectionID: fieldIndex.CollectionID,
			FieldID:      fieldIndex.FieldID,
			IndexID:      fieldIndex.IndexID,
		},
		PartitionID: fieldIndex.PartitionID,
		SegmentID:   fieldIndex.SegmentID,
		BuildID:     fieldIndex.BuildID,
		//EnableIndex:  fieldIndex.EnableIndex, // TODO populate it!!!
	}
}

func BatchConvertSegmentIndexDBToModel(fieldIndexes []*SegmentIndex) []*model.SegmentIndex {
	var indexes []*model.SegmentIndex
	for _, fi := range fieldIndexes {
		index := ConvertSegmentIndexDBToModel(fi)
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
