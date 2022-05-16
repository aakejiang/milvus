package kv

import (
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// model <---> etcdpb
func ConvertToFieldSchemaPB(field *model.Field) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:      field.FieldID,
		Name:         field.Name,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  field.Description,
		DataType:     field.DataType,
		TypeParams:   field.TypeParams,
		IndexParams:  field.IndexParams,
		AutoID:       field.AutoID,
	}
}

func BatchConvertToFieldSchemaPB(fields []*model.Field) []*schemapb.FieldSchema {
	fieldSchemas := make([]*schemapb.FieldSchema, len(fields))
	for idx, field := range fields {
		fieldSchemas[idx] = ConvertToFieldSchemaPB(field)
	}
	return fieldSchemas
}

func ConvertFieldPBToModel(fieldSchema *schemapb.FieldSchema) *model.Field {
	return &model.Field{
		FieldID:      fieldSchema.FieldID,
		Name:         fieldSchema.Name,
		IsPrimaryKey: fieldSchema.IsPrimaryKey,
		Description:  fieldSchema.Description,
		DataType:     fieldSchema.DataType,
		TypeParams:   fieldSchema.TypeParams,
		IndexParams:  fieldSchema.IndexParams,
		AutoID:       fieldSchema.AutoID,
	}
}

func BatchConvertFieldPBToModel(fieldSchemas []*schemapb.FieldSchema) []*model.Field {
	fields := make([]*model.Field, len(fieldSchemas))
	for idx, fieldSchema := range fieldSchemas {
		fields[idx] = ConvertFieldPBToModel(fieldSchema)
	}
	return fields
}

func ConvertCollectionPBToModel(coll *pb.CollectionInfo, extra map[string]string) *model.Collection {
	partitions := make([]*model.Partition, len(coll.PartitionIDs))
	for idx := range coll.PartitionIDs {
		partitions[idx] = &model.Partition{
			PartitionID:               coll.PartitionIDs[idx],
			PartitionName:             coll.PartitionNames[idx],
			PartitionCreatedTimestamp: coll.PartitionCreatedTimestamps[idx],
		}
	}
	indexes := make([]*model.Index, len(coll.FieldIndexes))
	for idx, fieldIndexInfo := range coll.FieldIndexes {
		indexes[idx] = &model.Index{
			FieldID: fieldIndexInfo.FiledID,
			IndexID: fieldIndexInfo.IndexID,
		}
	}
	return &model.Collection{
		CollectionID:         coll.ID,
		Name:                 coll.Schema.Name,
		Description:          coll.Schema.Description,
		AutoID:               coll.Schema.AutoID,
		Fields:               BatchConvertFieldPBToModel(coll.Schema.Fields),
		Partitions:           partitions,
		FieldIndexes:         indexes,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		Extra:                extra,
	}
}

func CloneCollectionModel(coll model.Collection) *model.Collection {
	return &model.Collection{
		CollectionID:         coll.CollectionID,
		Name:                 coll.Name,
		Description:          coll.Description,
		AutoID:               coll.AutoID,
		Fields:               coll.Fields,
		Partitions:           coll.Partitions,
		FieldIndexes:         coll.FieldIndexes,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		ShardsNum:            coll.ShardsNum,
		ConsistencyLevel:     coll.ConsistencyLevel,
		CreateTime:           coll.CreateTime,
		Extra:                coll.Extra,
	}
}

func ConvertToCollectionPB(coll *model.Collection) *pb.CollectionInfo {
	fields := make([]*schemapb.FieldSchema, len(coll.Fields))
	for idx, field := range coll.Fields {
		fields[idx] = &schemapb.FieldSchema{
			FieldID:      field.FieldID,
			Name:         field.Name,
			IsPrimaryKey: field.IsPrimaryKey,
			Description:  field.Description,
			DataType:     field.DataType,
			TypeParams:   field.TypeParams,
			IndexParams:  field.IndexParams,
			AutoID:       field.AutoID,
		}
	}
	collSchema := &schemapb.CollectionSchema{
		Name:        coll.Name,
		Description: coll.Description,
		AutoID:      coll.AutoID,
		Fields:      fields,
	}
	partitionIDs := make([]int64, len(coll.Partitions))
	partitionNames := make([]string, len(coll.Partitions))
	partitionCreatedTimestamps := make([]uint64, len(coll.Partitions))
	for idx, partition := range coll.Partitions {
		partitionIDs[idx] = partition.PartitionID
		partitionNames[idx] = partition.PartitionName
		partitionCreatedTimestamps[idx] = partition.PartitionCreatedTimestamp
	}
	fieldIndexes := make([]*pb.FieldIndexInfo, len(coll.FieldIndexes))
	for idx, index := range coll.FieldIndexes {
		fieldIndexes[idx] = &pb.FieldIndexInfo{
			FiledID: index.FieldID,
			IndexID: index.IndexID,
		}
	}
	return &pb.CollectionInfo{
		ID:                         coll.CollectionID,
		Schema:                     collSchema,
		PartitionIDs:               partitionIDs,
		PartitionNames:             partitionNames,
		FieldIndexes:               fieldIndexes,
		VirtualChannelNames:        coll.VirtualChannelNames,
		PhysicalChannelNames:       coll.PhysicalChannelNames,
		ShardsNum:                  coll.ShardsNum,
		PartitionCreatedTimestamps: partitionCreatedTimestamps,
		ConsistencyLevel:           coll.ConsistencyLevel,
		StartPositions:             coll.StartPositions,
	}
}

func ConvertToSegmentIndexPB(index *model.SegmentIndex) *pb.SegmentIndexInfo {
	return &pb.SegmentIndexInfo{
		CollectionID: index.CollectionID,
		PartitionID:  index.PartitionID,
		SegmentID:    index.SegmentID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		BuildID:      index.BuildID,
		EnableIndex:  index.EnableIndex,
	}
}

func ConvertSegmentIndexPBToModel(segIndex *pb.SegmentIndexInfo) *model.SegmentIndex {
	return &model.SegmentIndex{
		Index: model.Index{
			CollectionID: segIndex.CollectionID,
			FieldID:      segIndex.FieldID,
			IndexID:      segIndex.IndexID,
		},
		SegmentID:   segIndex.SegmentID,
		PartitionID: segIndex.PartitionID,
		BuildID:     segIndex.BuildID,
		EnableIndex: segIndex.EnableIndex,
	}
}

func ConvertIndexPBToModel(indexInfo *pb.IndexInfo) *model.Index {
	return &model.Index{
		IndexName:   indexInfo.IndexName,
		IndexID:     indexInfo.IndexID,
		IndexParams: indexInfo.IndexParams,
	}
}

func ConvertToIndexPB(index *model.Index) *pb.IndexInfo {
	return &pb.IndexInfo{
		IndexName:   index.IndexName,
		IndexID:     index.IndexID,
		IndexParams: index.IndexParams,
	}
}

func ConvertToCredentialPB(cred *model.Credential) *internalpb.CredentialInfo {
	if cred == nil {
		return nil
	}
	return &internalpb.CredentialInfo{
		Username:          cred.Username,
		EncryptedPassword: cred.EncryptedPassword,
	}
}
