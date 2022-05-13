package model

import (
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func ConvertToFieldSchemaPB(field *Field) *schemapb.FieldSchema {
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

func BatchConvertToFieldSchemaPB(fields []*Field) []*schemapb.FieldSchema {
	fieldSchemas := make([]*schemapb.FieldSchema, len(fields))
	for idx, field := range fields {
		fieldSchemas[idx] = ConvertToFieldSchemaPB(field)
	}
	return fieldSchemas
}

func ConvertFieldPBToModel(fieldSchema *schemapb.FieldSchema) *Field {
	return &Field{
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

func BatchConvertFieldPBToModel(fieldSchemas []*schemapb.FieldSchema) []*Field {
	fields := make([]*Field, len(fieldSchemas))
	for idx, fieldSchema := range fieldSchemas {
		fields[idx] = ConvertFieldPBToModel(fieldSchema)
	}
	return fields
}

func ConvertCollectionPBToModel(coll *pb.CollectionInfo, extra map[string]string) *Collection {
	partitions := make([]*Partition, len(coll.PartitionIDs))
	for idx := range coll.PartitionIDs {
		partitions[idx] = &Partition{
			PartitionID:               coll.PartitionIDs[idx],
			PartitionName:             coll.PartitionNames[idx],
			PartitionCreatedTimestamp: coll.PartitionCreatedTimestamps[idx],
		}
	}
	indexes := make([]*Index, len(coll.FieldIndexes))
	for idx, fieldIndexInfo := range coll.FieldIndexes {
		indexes[idx] = &Index{
			FieldID: fieldIndexInfo.FiledID,
			IndexID: fieldIndexInfo.IndexID,
		}
	}
	return &Collection{
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

func CloneCollectionModel(coll Collection) *Collection {
	return &Collection{
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

func ConvertToCollectionPB(coll *Collection) *pb.CollectionInfo {
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

func ConvertToSegmentIndexPB(index *Index) *pb.SegmentIndexInfo {
	return &pb.SegmentIndexInfo{
		CollectionID: index.CollectionID,
		PartitionID:  index.Segments[0].PartitionID,
		SegmentID:    index.Segments[0].SegmentID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		BuildID:      index.BuildID,
		EnableIndex:  index.EnableIndex,
	}
}

func ConvertSegmentIndexPBToModel(segIndex *pb.SegmentIndexInfo) *Index {
	return &Index{
		CollectionID: segIndex.CollectionID,
		Segments: []Segment{
			{
				SegmentID:   segIndex.SegmentID,
				PartitionID: segIndex.PartitionID,
			},
		},
		FieldID:     segIndex.FieldID,
		IndexID:     segIndex.IndexID,
		BuildID:     segIndex.BuildID,
		EnableIndex: segIndex.EnableIndex,
	}
}

func ConvertIndexPBToModel(indexInfo *pb.IndexInfo) *Index {
	return &Index{
		IndexName:   indexInfo.IndexName,
		IndexID:     indexInfo.IndexID,
		IndexParams: indexInfo.IndexParams,
	}
}

func ConvertToIndexPB(index *Index) *pb.IndexInfo {
	return &pb.IndexInfo{
		IndexName:   index.IndexName,
		IndexID:     index.IndexID,
		IndexParams: index.IndexParams,
	}
}

func ConvertToCredentialPB(cred *Credential) *internalpb.CredentialInfo {
	if cred == nil {
		return nil
	}
	return &internalpb.CredentialInfo{
		Username:          cred.Username,
		EncryptedPassword: cred.EncryptedPassword,
	}
}
