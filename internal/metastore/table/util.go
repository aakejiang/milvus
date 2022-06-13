package table

import (
	"encoding/json"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

func ConvertToCollectionProperties(collection *model.Collection) *CollProperties {
	return &CollProperties{
		VirtualChannelNames:  collection.VirtualChannelNames,
		PhysicalChannelNames: collection.PhysicalChannelNames,
		ShardsNum:            collection.ShardsNum,
		StartPositions:       collection.StartPositions,
		ConsistencyLevel:     collection.ConsistencyLevel,
	}
}

// model <---> db

func ConvertCollectionDBToModel(coll *Collection, partition *Partition, field *Field, index *Index) *model.Collection {
	properties := CollProperties{}
	if coll.Properties != nil {
		err := json.Unmarshal([]byte(*coll.Properties), &properties)
		if err != nil {
			log.Error("unmarshal collection properties error", zap.Error(err))
		}
	}
	var retDescription string
	if coll.Description != nil {
		retDescription = *coll.Description
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
		Description:          retDescription,
		AutoID:               coll.AutoID,
		Fields:               retFields,
		Partitions:           retPartitions,
		FieldIndexes:         retIndexes,
		VirtualChannelNames:  properties.VirtualChannelNames,
		PhysicalChannelNames: properties.PhysicalChannelNames,
		ShardsNum:            properties.ShardsNum,
		StartPositions:       properties.StartPositions,
		ConsistencyLevel:     properties.ConsistencyLevel,
		CreateTime:           coll.Ts,
	}
}

func ConvertCollectionsToIDMap(colls []*model.Collection) map[typeutil.UniqueID]*model.Collection {
	colMap := make(map[typeutil.UniqueID]*model.Collection)
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
	colMap := make(map[string]*model.Collection)
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
	var retDescription string
	if field.Description != nil {
		retDescription = *field.Description
	}
	var typeParams []commonpb.KeyValuePair
	if field.TypeParams != nil {
		err := json.Unmarshal([]byte(*field.TypeParams), &typeParams)
		if err != nil {
			log.Error("unmarshal TypeParams of field failed", zap.Error(err))
		}
	}
	var indexParams []commonpb.KeyValuePair
	if field.IndexParams != nil {
		err := json.Unmarshal([]byte(*field.IndexParams), &indexParams)
		if err != nil {
			log.Error("unmarshal IndexParams of field failed", zap.Error(err))
		}
	}
	return &model.Field{
		FieldID:      field.FieldID,
		Name:         field.FieldName,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  retDescription,
		DataType:     field.DataType,
		TypeParams:   funcutil.ConvertToKeyValuePairPointer(typeParams),
		IndexParams:  funcutil.ConvertToKeyValuePairPointer(indexParams),
		AutoID:       field.AutoID,
	}
}

func ConvertPartitionDBToModel(partiton *Partition) *model.Partition {
	return &model.Partition{
		PartitionID:               partiton.PartitionID,
		PartitionName:             partiton.PartitionName,
		PartitionCreatedTimestamp: partiton.PartitionCreatedTimestamp,
	}
}

func ConvertIndexDBToModel(index *Index) *model.Index {
	var indexParams []commonpb.KeyValuePair
	if index.IndexParams != "" {
		err := json.Unmarshal([]byte(index.IndexParams), &indexParams)
		if err != nil {
			log.Error("unmarshal IndexParams of field failed", zap.String("IndexParams", index.IndexParams), zap.Error(err))
		}
	}
	return &model.Index{
		CollectionID: index.CollectionID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		IndexName:    index.IndexName,
		IndexParams:  funcutil.ConvertToKeyValuePairPointer(indexParams),
	}
}

func ConvertSegmentIndexDBToModel(segmentIndex *SegmentIndex) *model.SegmentIndex {
	var indexFilePaths []string
	err := json.Unmarshal([]byte(segmentIndex.IndexFilePaths), indexFilePaths)
	if err != nil {
		log.Error("unmarshal IndexFilePaths of segment index failed", zap.Error(err))
	}
	return &model.SegmentIndex{
		Segment: model.Segment{
			SegmentID:   segmentIndex.SegmentID,
			PartitionID: segmentIndex.PartitionID,
		},
		EnableIndex:    segmentIndex.EnableIndex,
		BuildID:        segmentIndex.BuildID,
		IndexSize:      segmentIndex.IndexSize,
		IndexFilePaths: indexFilePaths,
	}
}

func ConvertToIndexModel(index *Index, segmentIndex *SegmentIndex) *model.Index {
	var indexParams []commonpb.KeyValuePair
	err := json.Unmarshal([]byte(index.IndexParams), &indexParams)
	if err != nil {
		log.Error("unmarshal IndexParams of field failed", zap.Error(err))
	}
	segIndex := ConvertSegmentIndexDBToModel(segmentIndex)
	return &model.Index{
		CollectionID: index.CollectionID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		IndexName:    index.IndexName,
		IndexParams:  funcutil.ConvertToKeyValuePairPointer(indexParams),
		SegmentIndexes: map[int64]model.SegmentIndex{
			segIndex.SegmentID: *segIndex,
		},
	}
}

func ConvertIndexesToMap(input []struct {
	Index
	SegmentIndex
}) map[string]*model.Index {
	idxMap := make(map[string]*model.Index)
	for _, record := range input {
		c := ConvertToIndexModel(&record.Index, &record.SegmentIndex)
		idxMap[c.IndexName] = c
	}
	return idxMap
}

func ConvertUserDBToModel(user *User) *model.Credential {
	return &model.Credential{
		Username:          user.Username,
		EncryptedPassword: user.EncryptedPassword,
	}
}

func ConvertDdOperationDBToModel(ddOp DdOperation) model.DdOperation {
	return model.DdOperation{
		Type:   ddOp.OperationType,
		Body:   ddOp.OperationBody,
		IsSent: ddOp.IsSent,
	}
}
