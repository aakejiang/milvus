package model

func CloneCollectionModel(coll Collection) *Collection {
	return &Collection{
		TenantID:             coll.TenantID,
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
		StartPositions:       coll.StartPositions,
		Extra:                coll.Extra,
	}
}

func MergeIndexModel(a *Index, b *Index) *Index {
	if b.SegmentIndexes != nil {
		if a.SegmentIndexes == nil {
			a.SegmentIndexes = b.SegmentIndexes
		} else {
			for segID, segmentIndex := range b.SegmentIndexes {
				a.SegmentIndexes[segID] = segmentIndex
			}
		}
	}

	if a.CollectionID == 0 && b.CollectionID != 0 {
		a.CollectionID = b.CollectionID
	}

	if a.FieldID == 0 && b.FieldID != 0 {
		a.FieldID = b.FieldID
	}

	if a.IndexID == 0 && b.IndexID != 0 {
		a.IndexID = b.IndexID
	}

	if a.IndexName == "" && b.IndexName != "" {
		a.IndexName = b.IndexName
	}

	if a.IndexParams == nil && b.IndexParams != nil {
		a.IndexParams = b.IndexParams
	}

	if a.Extra == nil && b.Extra != nil {
		a.Extra = b.Extra
	}

	return a
}
