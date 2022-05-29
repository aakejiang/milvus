// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"
	"math/rand"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestMetaTable_TableCatalog(t *testing.T) {
	const (
		collName        = "testColl"
		collNameInvalid = "testColl_invalid"
		aliasName1      = "alias1"
		aliasName2      = "alias2"
		partName        = "testPart"
		indexName       = "testColl_index_110"
	)

	collID := int64(rand.Uint64())
	collIDInvalid := int64(rand.Uint64())
	partIDDefault := int64(rand.Uint64())
	partID := int64(rand.Uint64())
	segID := int64(rand.Uint64())
	segID2 := int64(rand.Uint64())
	fieldID := int64(rand.Uint64())
	indexID := int64(rand.Uint64())
	buildID := int64(rand.Uint64())

	Params.Init()
	Params.CommonCfg.MetaStorageType = "database" // test table_catalog

	var vtso typeutil.Timestamp
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}

	mt, err := NewMetaTable(context.TODO(), nil, nil)
	assert.Nil(t, err)

	collInfo := &model.Collection{
		CollectionID: collID,
		Name:         collName,
		AutoID:       false,
		Fields: []*model.Field{
			{
				FieldID:      fieldID,
				Name:         "field110",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-k1",
						Value: "field110-v1",
					},
					{
						Key:   "field110-k2",
						Value: "field110-v2",
					},
				},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   "field110-i1",
						Value: "field110-v1",
					},
					{
						Key:   "field110-i2",
						Value: "field110-v2",
					},
				},
			},
		},
		FieldIndexes: []*model.Index{
			{
				FieldID: fieldID,
				IndexID: indexID,
			},
		},
		CreateTime: 0,
		Partitions: []*model.Partition{
			{
				PartitionID:               partIDDefault,
				PartitionName:             Params.CommonCfg.DefaultPartitionName,
				PartitionCreatedTimestamp: 0,
			},
		},
	}

	idxInfo := []*model.Index{
		{
			IndexName: indexName,
			IndexID:   indexID,
			IndexParams: []*commonpb.KeyValuePair{
				{
					Key:   "field110-i1",
					Value: "field110-v1",
				},
				{
					Key:   "field110-i2",
					Value: "field110-v2",
				},
			},
		},
	}

	mt.indexID2Meta[indexID] = *idxInfo[0]
	ts := ftso()

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("add collection", func(t *testing.T) {
		defer wg.Done()

		err = mt.AddCollection(collInfo, ts, model.DdOperation{
			Body: "Ggh0ZXN0Q29sbCIIdGVzdFBhcnQwATgU",
			Type: "CreateCollection",
		})
		assert.Nil(t, err)
		assert.Equal(t, uint64(1), ts)

		collMeta, err := mt.GetCollectionByName(collName, 0)
		assert.Nil(t, err)
		assert.Equal(t, collMeta.CreateTime, ts)
		assert.Equal(t, collMeta.Partitions[0].PartitionCreatedTimestamp, ts)

		assert.Equal(t, partIDDefault, collMeta.Partitions[0].PartitionID)
		assert.Equal(t, 1, len(collMeta.Partitions))
		assert.True(t, mt.HasCollection(collInfo.CollectionID, 0))

		field, err := mt.GetFieldSchema(collName, "field110")
		assert.Nil(t, err)
		assert.Equal(t, collInfo.Fields[0].FieldID, field.FieldID)

		// check DD operation flag
		flag, err := mt.IsDDMsgSent()
		assert.Nil(t, err)
		assert.Equal(t, false, flag)
	})

	wg.Add(1)
	t.Run("add alias", func(t *testing.T) {
		defer wg.Done()

		exists := mt.IsAlias(aliasName1)
		assert.False(t, exists)
		err = mt.AddAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		aliases := mt.ListAliases(collID)
		assert.Equal(t, aliases, []string{aliasName1})
		exists = mt.IsAlias(aliasName1)
		assert.True(t, exists)
	})
	wg.Add(1)
	t.Run("alter alias", func(t *testing.T) {
		defer wg.Done()

		err = mt.AlterAlias(aliasName1, collName, ts)
		assert.Nil(t, err)
		err = mt.AlterAlias(aliasName1, collNameInvalid, ts)
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("delete alias", func(t *testing.T) {
		defer wg.Done()

		err = mt.DropAlias(aliasName1, ts)
		assert.Nil(t, err)
	})

	wg.Add(1)
	t.Run("add partition", func(t *testing.T) {
		defer wg.Done()

		err = mt.AddPartition(collID, partName, partID, ts, model.DdOperation{})
		assert.Nil(t, err)
		//assert.Equal(t, ts, uint64(2))

		collMeta, ok := mt.collID2Meta[collID]
		assert.True(t, ok)
		assert.Equal(t, 2, len(collMeta.Partitions))
		assert.Equal(t, collMeta.Partitions[1].PartitionName, partName)
		assert.Equal(t, ts, collMeta.Partitions[1].PartitionCreatedTimestamp)

		// check DD operation flag
		flag, err := mt.IsDDMsgSent()
		assert.Nil(t, err)
		assert.Equal(t, false, flag)
	})

	wg.Add(1)
	t.Run("add segment index", func(t *testing.T) {
		defer wg.Done()
		index := model.Index{
			CollectionID: collID,
			FieldID:      fieldID,
			IndexID:      indexID,
			SegmentIndexes: []model.SegmentIndex{
				{
					Segment: model.Segment{
						SegmentID:   segID,
						PartitionID: partID,
					},
					BuildID: buildID,
				},
			},
		}
		err = mt.AlterIndex(&index)
		assert.Nil(t, err)

		// it's legal to add index twice
		err = mt.AlterIndex(&index)
		assert.Nil(t, err)
	})

	wg.Add(1)
	t.Run("add diff index with same index name", func(t *testing.T) {
		defer wg.Done()
		params := []*commonpb.KeyValuePair{
			{
				Key:   "field110-k1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-k2",
				Value: "field110-v2",
			},
		}
		idxInfo := &model.Index{
			IndexName:   "testColl_index_110",
			IndexID:     indexID,
			IndexParams: params,
		}

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo, nil)
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("get not indexed segments", func(t *testing.T) {
		defer wg.Done()
		params := []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-i2",
				Value: "field110-v2",
			},
		}

		tparams := []*commonpb.KeyValuePair{
			{
				Key:   "field110-k1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-k2",
				Value: "field110-v2",
			},
		}
		idxInfo := &model.Index{
			IndexName:   "field110",
			IndexID:     2000,
			IndexParams: params,
		}

		_, _, err := mt.GetNotIndexedSegments("collTest", "field110", idxInfo, nil)
		assert.NotNil(t, err)
		seg, field, err := mt.GetNotIndexedSegments(collName, "field110", idxInfo, []typeutil.UniqueID{segID, segID2})
		assert.Nil(t, err)
		assert.Equal(t, 1, len(seg))
		assert.Equal(t, segID2, seg[0])
		assert.True(t, metastore.EqualKeyPairArray(field.TypeParams, tparams))

		params = []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
		}
		idxInfo.IndexParams = params
		idxInfo.IndexID = 2001
		idxInfo.IndexName = "field110-1"

		seg, field, err = mt.GetNotIndexedSegments(collName, "field110", idxInfo, []typeutil.UniqueID{segID, segID2})
		assert.Nil(t, err)
		assert.Equal(t, 2, len(seg))
		assert.Equal(t, segID, seg[0])
		assert.Equal(t, segID2, seg[1])
		assert.True(t, metastore.EqualKeyPairArray(field.TypeParams, tparams))
	})

	wg.Add(1)
	t.Run("get index by name", func(t *testing.T) {
		defer wg.Done()
		_, idx, err := mt.GetIndexByName(collName, indexName)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(idx))
		assert.Equal(t, idxInfo[0].IndexID, idx[0].IndexID)
		params := []*commonpb.KeyValuePair{
			{
				Key:   "field110-i1",
				Value: "field110-v1",
			},
			{
				Key:   "field110-i2",
				Value: "field110-v2",
			},
		}
		assert.True(t, metastore.EqualKeyPairArray(idx[0].IndexParams, params))

		_, idx, err = mt.GetIndexByName(collName, "idx201")
		assert.Nil(t, err)
		assert.Zero(t, len(idx))
	})

	wg.Add(1)
	t.Run("drop index", func(t *testing.T) {
		defer wg.Done()
		idx, ok, err := mt.DropIndex(collName, "field110", indexName)
		assert.Nil(t, err)
		assert.True(t, ok)
		assert.Equal(t, idxInfo[0].IndexID, idx)

		_, ok, err = mt.DropIndex(collName, "field110", "field110-error")
		assert.Nil(t, err)
		assert.False(t, ok)

		_, idxs, err := mt.GetIndexByName(collName, "field110")
		assert.Nil(t, err)
		assert.Zero(t, len(idxs))

		_, err = mt.GetSegmentIndexInfoByID(segID, -1, "")
		assert.NotNil(t, err)
	})

	wg.Add(1)
	t.Run("drop partition", func(t *testing.T) {
		defer wg.Done()
		id, err := mt.DeletePartition(collID, partName, ts, model.DdOperation{})
		assert.Nil(t, err)
		assert.Equal(t, partID, id)

		// check DD operation flag
		//flag, err := mt.txn.Load(metastore.DDMsgSendPrefix)
		//assert.Nil(t, err)
		//assert.Equal(t, "false", flag)
	})

	wg.Add(1)
	t.Run("drop collection", func(t *testing.T) {
		defer wg.Done()
		err = mt.DeleteCollection(collIDInvalid, ts, model.DdOperation{})
		assert.NotNil(t, err)
		err = mt.AddAlias(aliasName2, collName, ts)
		assert.Nil(t, err)
		err = mt.DeleteCollection(collID, ts, model.DdOperation{})
		assert.Nil(t, err)
		err = mt.DropAlias(aliasName2, ts)
		assert.NotNil(t, err)

		// check DD operation flag
		//flag, err := mt.txn.Load(metastore.DDMsgSendPrefix)
		//assert.Nil(t, err)
		//assert.Equal(t, "false", flag)
	})

	wg.Add(1)
	t.Run("delete credential", func(t *testing.T) {
		defer wg.Done()

		err = mt.DeleteCredential("")
		assert.Nil(t, err)

		err = mt.DeleteCredential("abcxyz")
		assert.Nil(t, err)
	})

	wg.Wait()
}

func TestMetaWithTimestamp_TableCatalog(t *testing.T) {
	const (
		collName1 = "t1"
		collName2 = "t2"
		partName1 = "p1"
		partName2 = "p2"
	)

	collID1 := int64(rand.Uint64())
	collID2 := int64(rand.Uint64())
	partID1 := int64(rand.Uint64())
	partID2 := int64(rand.Uint64())
	fieldID1 := int64(rand.Uint64())
	fieldID2 := int64(rand.Uint64())

	Params.Init()
	Params.CommonCfg.MetaStorageType = "database" // test table_catalog

	var tsoStart typeutil.Timestamp = 100
	vtso := tsoStart
	ftso := func() typeutil.Timestamp {
		vtso++
		return vtso
	}

	mt, err := NewMetaTable(context.TODO(), nil, nil)
	assert.Nil(t, err)

	coll1 := &model.Collection{
		CollectionID: collID1,
		Name:         collName1,
		Fields: []*model.Field{
			{
				FieldID:      fieldID1,
				Name:         "field1",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
			},
		},
	}

	coll2 := &model.Collection{
		CollectionID: collID2,
		Name:         collName2,
		Fields: []*model.Field{
			{
				FieldID:      fieldID2,
				Name:         "field2",
				IsPrimaryKey: false,
				Description:  "",
				DataType:     schemapb.DataType_FloatVector,
			},
		},
	}
	t1 := ftso()
	t2 := ftso()

	// drop collection first
	defer func() {
		mt.DeleteCollection(collID1, t1, model.DdOperation{})
		delete(mt.collName2ID, collName1)
		mt.DeleteCollection(collID2, t2, model.DdOperation{})
		delete(mt.collName2ID, collName2)
	}()

	// create collection 1
	coll1.Partitions = []*model.Partition{{PartitionID: partID1, PartitionName: partName1, PartitionCreatedTimestamp: ftso()}}
	err = mt.AddCollection(coll1, t1, model.DdOperation{})
	assert.Nil(t, err)

	// create collection 2
	coll2.Partitions = []*model.Partition{{PartitionID: partID2, PartitionName: partName2, PartitionCreatedTimestamp: ftso()}}
	err = mt.AddCollection(coll2, t2, model.DdOperation{})
	assert.Nil(t, err)

	assert.True(t, mt.HasCollection(collID1, 0))
	assert.True(t, mt.HasCollection(collID2, 0))

	assert.True(t, mt.HasCollection(collID1, t2))
	assert.True(t, mt.HasCollection(collID2, t2))

	assert.True(t, mt.HasCollection(collID1, t1))
	assert.False(t, mt.HasCollection(collID2, t1))

	assert.False(t, mt.HasCollection(collID1, tsoStart))
	assert.False(t, mt.HasCollection(collID2, tsoStart))

	c1, err := mt.GetCollectionByID(collID1, 0)
	assert.Nil(t, err)
	c2, err := mt.GetCollectionByID(collID2, 0)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t2)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByID(collID2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)

	c1, err = mt.GetCollectionByID(collID1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByID(collID2, tsoStart)
	assert.NotNil(t, err)

	c1, err = mt.GetCollectionByName(collName1, 0)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, t2)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)
	assert.Equal(t, collID2, c2.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, t1)
	assert.Nil(t, err)
	c2, err = mt.GetCollectionByName(collName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, collID1, c1.CollectionID)

	c1, err = mt.GetCollectionByName(collName1, tsoStart)
	assert.NotNil(t, err)
	c2, err = mt.GetCollectionByName(collName2, tsoStart)
	assert.NotNil(t, err)

	getKeys := func(m map[string]*model.Collection) []string {
		keys := make([]string, 0, len(m))
		for key := range m {
			keys = append(keys, key)
		}
		return keys
	}

	s1, err := mt.ListCollections(0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1, collName2})

	s1, err = mt.ListCollections(t2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1, collName2})

	s1, err = mt.ListCollections(t1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(s1))
	assert.ElementsMatch(t, getKeys(s1), []string{collName1})

	s1, err = mt.ListCollections(tsoStart)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s1))

	p1, err := mt.GetPartitionByName(collID1, partName1, 0)
	assert.Nil(t, err)
	p2, err := mt.GetPartitionByName(collID2, partName2, 0)
	assert.Nil(t, err)
	assert.Equal(t, partID1, p1)
	assert.Equal(t, partID2, p2)

	p1, err = mt.GetPartitionByName(collID1, partName1, t2)
	assert.Nil(t, err)
	p2, err = mt.GetPartitionByName(collID2, partName2, t2)
	assert.Nil(t, err)
	assert.Equal(t, partID1, p1)
	assert.Equal(t, partID2, p2)

	p1, err = mt.GetPartitionByName(collID1, partName1, t1)
	assert.Nil(t, err)
	_, err = mt.GetPartitionByName(collID2, partName2, t1)
	assert.NotNil(t, err)
	assert.Equal(t, partID1, p1)

	_, err = mt.GetPartitionByName(collID1, partName1, tsoStart)
	assert.NotNil(t, err)
	_, err = mt.GetPartitionByName(collID2, partName2, tsoStart)
	assert.NotNil(t, err)

	var cID UniqueID
	cID, err = mt.GetCollectionIDByName(collName1)
	assert.NoError(t, err)
	assert.Equal(t, collID1, cID)

	_, err = mt.GetCollectionIDByName("badname")
	assert.Error(t, err)
}
