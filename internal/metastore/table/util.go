package table

import (
	"encoding/json"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"go.uber.org/zap"
)

// model <---> db

func ConvertCollectionDBToModel(coll *Collection) *model.Collection {
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
	return &model.Collection{
		CollectionID: coll.CollectionID,
		Name:         coll.CollectionName,
		Description:  coll.Description,
		AutoID:       coll.AutoID,
		//Fields:               , // TODO populate it!!!
		//Partitions:           , // TODO populate it!!!
		//FieldIndexes:         , // TODO populate it!!!
		VirtualChannelNames:  properties.VirtualChannelNames,
		PhysicalChannelNames: properties.PhysicalChannelNames,
		ShardsNum:            properties.ShardsNum,
		StartPositions:       properties.StartPositions,
		ConsistencyLevel:     properties.ConsistencyLevel,
		CreateTime:           coll.CreatedTime,
		Aliases:              aliases,
	}
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
		indexDB := ConvertIndexDBToModel(fi)
		indexes = append(indexes, indexDB)
	}
	return indexes
}

func ConvertUserDBToModel(user *User) *model.Credential {
	return &model.Credential{
		Username:          user.Username,
		EncryptedPassword: user.EncryptedPassword,
	}
}
