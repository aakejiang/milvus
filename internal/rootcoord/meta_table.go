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
	"bytes"
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	// ProxyMetaPrefix prefix for proxy meta
	ProxyMetaPrefix = ComponentPrefix + "/proxy"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	// SegmentIndexMetaPrefix prefix for segment index meta
	SegmentIndexMetaPrefix = ComponentPrefix + "/segment-index"

	// IndexMetaPrefix prefix for index meta
	IndexMetaPrefix = ComponentPrefix + "/index"

	// CollectionAliasMetaPrefix prefix for collection alias meta
	CollectionAliasMetaPrefix = ComponentPrefix + "/collection-alias"

	// TimestampPrefix prefix for timestamp
	TimestampPrefix = ComponentPrefix + "/timestamp"

	// DDOperationPrefix prefix for DD operation
	DDOperationPrefix = ComponentPrefix + "/dd-operation"

	// DDMsgSendPrefix prefix to indicate whether DD msg has been send
	DDMsgSendPrefix = ComponentPrefix + "/dd-msg-send"

	// CreateCollectionDDType name of DD type for create collection
	CreateCollectionDDType = "CreateCollection"

	// DropCollectionDDType name of DD type for drop collection
	DropCollectionDDType = "DropCollection"

	// CreatePartitionDDType name of DD type for create partition
	CreatePartitionDDType = "CreatePartition"

	// DropPartitionDDType name of DD type for drop partition
	DropPartitionDDType = "DropPartition"

	// UserSubPrefix subpath for credential user
	UserSubPrefix = "/credential/users"

	// CredentialPrefix prefix for credential user
	CredentialPrefix = ComponentPrefix + UserSubPrefix

	// DefaultIndexType name of default index type for scalar field
	DefaultIndexType = "STL_SORT"

	// DefaultStringIndexType name of default index type for varChar/string field
	DefaultStringIndexType = "Trie"
)

// MetaTable store all rootCoord meta info
type MetaTable struct {
	ctx      context.Context
	txn      kv.TxnKV      // client of a reliable txnkv service, i.e. etcd client
	snapshot kv.SnapShotKV // client of a reliable snapshotkv service, i.e. etcd client
	catalog  Catalog

	proxyID2Meta    map[typeutil.UniqueID]pb.ProxyMeta                              // proxy id to proxy meta
	collID2Meta     map[typeutil.UniqueID]model.Collection                          // collection id -> collection meta
	collName2ID     map[string]typeutil.UniqueID                                    // collection name to collection id
	collAlias2ID    map[string]typeutil.UniqueID                                    // collection alias to collection id
	partID2SegID    map[typeutil.UniqueID]map[typeutil.UniqueID]bool                // partition id -> segment_id -> bool
	segID2IndexMeta map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo // collection id/index_id/partition_id/segment_id -> meta
	indexID2Meta    map[typeutil.UniqueID]pb.IndexInfo                              // collection id/index_id -> meta

	proxyLock sync.RWMutex
	ddLock    sync.RWMutex
	credLock  sync.RWMutex
}

// NewMetaTable creates meta table for rootcoord, which stores all in-memory information
// for collection, partition, segment, index etc.
func NewMetaTable(ctx context.Context, txn kv.TxnKV, snap kv.SnapShotKV) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:       ctx,
		txn:       txn,
		snapshot:  snap,
		catalog:   &KVCatalog{txn: txn, snapshot: snap},
		proxyLock: sync.RWMutex{},
		ddLock:    sync.RWMutex{},
		credLock:  sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reloadFromKV() error {
	mt.proxyID2Meta = make(map[typeutil.UniqueID]pb.ProxyMeta)
	mt.collID2Meta = make(map[typeutil.UniqueID]model.Collection)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.collAlias2ID = make(map[string]typeutil.UniqueID)
	mt.partID2SegID = make(map[typeutil.UniqueID]map[typeutil.UniqueID]bool)
	mt.segID2IndexMeta = make(map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo)
	mt.indexID2Meta = make(map[typeutil.UniqueID]pb.IndexInfo)

	_, values, err := mt.txn.LoadWithPrefix(ProxyMetaPrefix)
	if err != nil {
		return err
	}

	for _, value := range values {
		if bytes.Equal([]byte(value), suffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		proxyMeta := pb.ProxyMeta{}
		err = proto.Unmarshal([]byte(value), &proxyMeta)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.ProxyMeta err:%w", err)
		}
		mt.proxyID2Meta[proxyMeta.ID] = proxyMeta
	}

	collMap, err := mt.catalog.ListCollections(mt.ctx, 0)
	if err != nil {
		return err
	}
	for _, coll := range collMap {
		mt.collID2Meta[coll.CollectionID] = *coll
		mt.collName2ID[coll.Name] = coll.CollectionID
	}

	_, values, err = mt.txn.LoadWithPrefix(SegmentIndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		if bytes.Equal([]byte(value), suffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.Unmarshal([]byte(value), &segmentIndexInfo)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.SegmentIndexInfo err:%w", err)
		}

		// update partID2SegID
		segIDMap, ok := mt.partID2SegID[segmentIndexInfo.PartitionID]
		if ok {
			segIDMap[segmentIndexInfo.SegmentID] = true
		} else {
			idMap := make(map[typeutil.UniqueID]bool)
			idMap[segmentIndexInfo.SegmentID] = true
			mt.partID2SegID[segmentIndexInfo.PartitionID] = idMap
		}

		// update segID2IndexMeta
		idx, ok := mt.segID2IndexMeta[segmentIndexInfo.SegmentID]
		if ok {
			idx[segmentIndexInfo.IndexID] = segmentIndexInfo
		} else {
			meta := make(map[typeutil.UniqueID]pb.SegmentIndexInfo)
			meta[segmentIndexInfo.IndexID] = segmentIndexInfo
			mt.segID2IndexMeta[segmentIndexInfo.SegmentID] = meta
		}
	}

	_, values, err = mt.txn.LoadWithPrefix(IndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		if bytes.Equal([]byte(value), suffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		meta := pb.IndexInfo{}
		err = proto.Unmarshal([]byte(value), &meta)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.IndexInfo err:%w", err)
		}
		mt.indexID2Meta[meta.IndexID] = meta
	}

	_, values, err = mt.snapshot.LoadWithPrefix(CollectionAliasMetaPrefix, 0)
	if err != nil {
		return err
	}
	for _, value := range values {
		aliasInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &aliasInfo)
		if err != nil {
			return fmt.Errorf("rootcoord Unmarshal pb.AliasInfo err:%w", err)
		}
		mt.collAlias2ID[aliasInfo.Schema.Name] = aliasInfo.ID
	}

	log.Debug("reload meta table from KV successfully")
	return nil
}

// AddProxy add proxy
func (mt *MetaTable) AddProxy(po *pb.ProxyMeta) error {
	mt.proxyLock.Lock()
	defer mt.proxyLock.Unlock()

	k := fmt.Sprintf("%s/%d", ProxyMetaPrefix, po.ID)
	v, err := proto.Marshal(po)
	if err != nil {
		log.Error("Failed to marshal ProxyMeta in AddProxy", zap.Error(err))
		return err
	}

	err = mt.txn.Save(k, string(v))
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}
	mt.proxyID2Meta[po.ID] = *po
	return nil
}

// AddCollection add collection
func (mt *MetaTable) AddCollection(coll *model.Collection, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if _, ok := mt.collName2ID[coll.Name]; ok {
		return fmt.Errorf("collection %s exist", coll.Name)
	}

	coll.CreateTime = ts
	if len(coll.Partitions) == 1 {
		coll.Partitions[0].PartitionCreatedTimestamp = ts
	}

	mt.collID2Meta[coll.CollectionID] = *coll
	mt.collName2ID[coll.Name] = coll.CollectionID

	meta := map[string]string{}
	meta[DDMsgSendPrefix] = "false"
	meta[DDOperationPrefix] = ddOpStr
	coll.Extra = meta
	return mt.catalog.CreateCollection(mt.ctx, coll, ts)
}

// DeleteCollection delete collection
func (mt *MetaTable) DeleteCollection(collID typeutil.UniqueID, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	col, ok := mt.collID2Meta[collID]
	if !ok {
		return fmt.Errorf("can't find collection. id = %d", collID)
	}

	delete(mt.collID2Meta, collID)
	delete(mt.collName2ID, col.Name)

	// update segID2IndexMeta
	for _, partition := range col.Partitions {
		partID := partition.PartitionID
		if segIDMap, ok := mt.partID2SegID[typeutil.UniqueID(partID)]; ok {
			for segID := range segIDMap {
				delete(mt.segID2IndexMeta, segID)
			}
		}
		delete(mt.partID2SegID, typeutil.UniqueID(partID))
	}

	for _, idxInfo := range col.FieldIndexes {
		_, ok := mt.indexID2Meta[idxInfo.IndexID]
		if !ok {
			log.Warn("index id not exist", zap.Int64("index id", idxInfo.IndexID))
			continue
		}
		delete(mt.indexID2Meta, idxInfo.IndexID)
	}
	var aliases []string
	// delete collection aliases
	for alias, cid := range mt.collAlias2ID {
		if cid == collID {
			aliases = append(aliases, alias)
			delete(mt.collAlias2ID, alias)
		}
	}

	// save ddOpStr into etcd
	var meta = map[string]string{
		DDMsgSendPrefix:   "false",
		DDOperationPrefix: ddOpStr,
	}

	collection := &model.Collection{
		CollectionID: collID,
		Aliases:      aliases,
		Extra:        meta,
	}

	return mt.catalog.DropCollection(mt.ctx, collection, ts)
}

// HasCollection return collection existence
func (mt *MetaTable) HasCollection(collID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	if ts == 0 {
		_, ok := mt.collID2Meta[collID]
		return ok
	}

	return mt.catalog.CollectionExists(mt.ctx, collID, ts)
}

// GetCollectionByID return collection meta by collection id
func (mt *MetaTable) GetCollectionByID(collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		col, ok := mt.collID2Meta[collectionID]
		if !ok {
			return nil, fmt.Errorf("can't find collection id : %d", collectionID)
		}
		return model.CloneCollectionModel(col), nil
	}

	return mt.catalog.GetCollectionByID(mt.ctx, collectionID, ts)
}

// GetCollectionByName return collection meta by collection name
func (mt *MetaTable) GetCollectionByName(collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if ts == 0 {
		vid, ok := mt.collName2ID[collectionName]
		if !ok {
			if vid, ok = mt.collAlias2ID[collectionName]; !ok {
				return nil, fmt.Errorf("can't find collection: " + collectionName)
			}
		}
		col, ok := mt.collID2Meta[vid]
		if !ok {
			return nil, fmt.Errorf("can't find collection %s with id %d", collectionName, vid)
		}

		return model.CloneCollectionModel(col), nil
	}

	return mt.catalog.GetCollectionByName(mt.ctx, collectionName, ts)
}

// ListCollections list all collection names
func (mt *MetaTable) ListCollections(ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	cols := make(map[string]*model.Collection)

	if ts == 0 {
		for collName, collID := range mt.collName2ID {
			col := mt.collID2Meta[collID]
			cols[collName] = model.CloneCollectionModel(col)
		}
		return cols, nil
	}

	return mt.catalog.ListCollections(mt.ctx, ts)
}

// ListAliases list all collection aliases
func (mt *MetaTable) ListAliases(collID typeutil.UniqueID) []string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	var aliases []string
	for alias, cid := range mt.collAlias2ID {
		if cid == collID {
			aliases = append(aliases, alias)
		}
	}
	return aliases
}

// ListCollectionVirtualChannels list virtual channels of all collections
func (mt *MetaTable) ListCollectionVirtualChannels() map[typeutil.UniqueID][]string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	chanMap := make(map[typeutil.UniqueID][]string)

	for id, collInfo := range mt.collID2Meta {
		chanMap[id] = collInfo.VirtualChannelNames
	}
	return chanMap
}

// ListCollectionPhysicalChannels list physical channels of all collections
func (mt *MetaTable) ListCollectionPhysicalChannels() map[typeutil.UniqueID][]string {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	chanMap := make(map[typeutil.UniqueID][]string)

	for id, collInfo := range mt.collID2Meta {
		chanMap[id] = collInfo.PhysicalChannelNames
	}
	return chanMap
}

// AddPartition add partition
func (mt *MetaTable) AddPartition(collID typeutil.UniqueID, partitionName string, partitionID typeutil.UniqueID, ts typeutil.Timestamp, ddOpStr string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	coll, ok := mt.collID2Meta[collID]
	if !ok {
		return fmt.Errorf("can't find collection. id = %d", collID)
	}

	// number of partition tags (except _default) should be limited to 4096 by default
	if int64(len(coll.Partitions)) >= Params.RootCoordCfg.MaxPartitionNum {
		return fmt.Errorf("maximum partition's number should be limit to %d", Params.RootCoordCfg.MaxPartitionNum)
	}

	for _, p := range coll.Partitions {
		if p.PartitionID == partitionID {
			return fmt.Errorf("partition id = %d already exists", partitionID)
		}
		if p.PartitionName == partitionName {
			return fmt.Errorf("partition name = %s already exists", partitionName)
		}
		// no necessary to check created timestamp
	}

	coll.Partitions = append(coll.Partitions,
		model.Partition{
			PartitionID:               partitionID,
			PartitionName:             partitionName,
			PartitionCreatedTimestamp: ts,
		})
	mt.collID2Meta[collID] = coll

	metaTxn := map[string]string{}
	// save ddOpStr into etcd
	metaTxn[DDMsgSendPrefix] = "false"
	metaTxn[DDOperationPrefix] = ddOpStr
	partition := &model.Partition{
		Extra: metaTxn,
	}
	return mt.catalog.CreatePartition(mt.ctx, &coll, partition, ts)
}

// GetPartitionNameByID return partition name by partition id
func (mt *MetaTable) GetPartitionNameByID(collID, partitionID typeutil.UniqueID, ts typeutil.Timestamp) (string, error) {
	if ts == 0 {
		mt.ddLock.RLock()
		defer mt.ddLock.RUnlock()
		col, ok := mt.collID2Meta[collID]
		if !ok {
			return "", fmt.Errorf("can't find collection id = %d", collID)
		}
		for _, partition := range col.Partitions {
			if partition.PartitionID == partitionID {
				return partition.PartitionName, nil
			}
		}
		return "", fmt.Errorf("partition %d does not exist", partitionID)
	}

	col, err := mt.catalog.GetCollectionByID(mt.ctx, collID, ts)
	if err != nil {
		return "", err
	}
	for _, partition := range col.Partitions {
		if partition.PartitionID == partitionID {
			return partition.PartitionName, nil
		}
	}
	return "", fmt.Errorf("partition %d does not exist", partitionID)
}

func (mt *MetaTable) getPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	if ts == 0 {
		col, ok := mt.collID2Meta[collID]
		if !ok {
			return 0, fmt.Errorf("can't find collection id = %d", collID)
		}
		for _, partition := range col.Partitions {
			if partition.PartitionName == partitionName {
				return partition.PartitionID, nil
			}
		}
		return 0, fmt.Errorf("partition %s does not exist", partitionName)
	}

	col, err := mt.catalog.GetCollectionByID(mt.ctx, collID, ts)
	if err != nil {
		return 0, err
	}
	for _, partition := range col.Partitions {
		if partition.PartitionName == partitionName {
			return partition.PartitionID, nil
		}
	}
	return 0, fmt.Errorf("partition %s does not exist", partitionName)
}

// GetPartitionByName return partition id by partition name
func (mt *MetaTable) GetPartitionByName(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.getPartitionByName(collID, partitionName, ts)
}

// HasPartition check partition existence
func (mt *MetaTable) HasPartition(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, err := mt.getPartitionByName(collID, partitionName, ts)
	return err == nil
}

// DeletePartition delete partition
func (mt *MetaTable) DeletePartition(collID typeutil.UniqueID, partitionName string, ts typeutil.Timestamp, ddOpStr string) (typeutil.UniqueID, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if partitionName == Params.CommonCfg.DefaultPartitionName {
		return 0, fmt.Errorf("default partition cannot be deleted")
	}

	col, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, fmt.Errorf("can't find collection id = %d", collID)
	}

	// check tag exists
	exist := false

	parts := make([]model.Partition, 0, len(col.Partitions))

	var partID typeutil.UniqueID
	for _, partition := range col.Partitions {
		if partition.PartitionName == partitionName {
			partID = partition.PartitionID
			exist = true
		} else {
			parts = append(parts, partition)
		}
	}
	if !exist {
		return 0, fmt.Errorf("partition %s does not exist", partitionName)
	}
	col.Partitions = parts
	mt.collID2Meta[collID] = col

	// update segID2IndexMeta and partID2SegID
	if segIDMap, ok := mt.partID2SegID[partID]; ok {
		for segID := range segIDMap {
			delete(mt.segID2IndexMeta, segID)
		}
	}
	delete(mt.partID2SegID, partID)

	metaTxn := make(map[string]string)
	// save ddOpStr into etcd
	metaTxn[DDMsgSendPrefix] = "false"
	metaTxn[DDOperationPrefix] = ddOpStr
	col.Extra = metaTxn

	err := mt.catalog.DropPartition(mt.ctx, &col, partID, ts)
	if err != nil {
		return 0, err
	}

	return partID, nil
}

// AddIndex add index
func (mt *MetaTable) AddIndex(segIdxInfo *pb.SegmentIndexInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	col, ok := mt.collID2Meta[segIdxInfo.CollectionID]
	if !ok {
		return fmt.Errorf("collection id = %d not found", segIdxInfo.CollectionID)
	}
	exist := false

	for _, fidx := range col.FieldIndexes {
		if fidx.IndexID == segIdxInfo.IndexID {
			exist = true
			break
		}
	}
	if !exist {
		return fmt.Errorf("index id = %d not found", segIdxInfo.IndexID)
	}

	segIdxMap, ok := mt.segID2IndexMeta[segIdxInfo.SegmentID]
	if !ok {
		idxMap := map[typeutil.UniqueID]pb.SegmentIndexInfo{segIdxInfo.IndexID: *segIdxInfo}
		mt.segID2IndexMeta[segIdxInfo.SegmentID] = idxMap

		segIDMap := map[typeutil.UniqueID]bool{segIdxInfo.SegmentID: true}
		mt.partID2SegID[segIdxInfo.PartitionID] = segIDMap
	} else {
		tmpInfo, ok := segIdxMap[segIdxInfo.IndexID]
		if ok {
			if SegmentIndexInfoEqual(segIdxInfo, &tmpInfo) {
				if segIdxInfo.BuildID == tmpInfo.BuildID {
					log.Debug("Identical SegmentIndexInfo already exist", zap.Int64("IndexID", segIdxInfo.IndexID))
					return nil
				}
				return fmt.Errorf("index id = %d exist", segIdxInfo.IndexID)
			}
		}
	}

	mt.segID2IndexMeta[segIdxInfo.SegmentID][segIdxInfo.IndexID] = *segIdxInfo
	mt.partID2SegID[segIdxInfo.PartitionID][segIdxInfo.SegmentID] = true

	index := &model.SegmentIndex{
		CollectionID: segIdxInfo.CollectionID,
		PartitionID:  segIdxInfo.PartitionID,
		SegmentID:    segIdxInfo.SegmentID,
		FieldID:      segIdxInfo.FieldID,
		IndexID:      segIdxInfo.IndexID,
		BuildID:      segIdxInfo.BuildID,
		EnableIndex:  segIdxInfo.EnableIndex,
	}
	return mt.catalog.CreateIndex(mt.ctx, index)
}

// DropIndex drop index
func (mt *MetaTable) DropIndex(collName, fieldName, indexName string) (typeutil.UniqueID, bool, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return 0, false, fmt.Errorf("collection name = %s not exist", collName)
		}
	}
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, false, fmt.Errorf("collection name  = %s not has meta", collName)
	}
	fieldSch, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return 0, false, err
	}
	fieldIdxInfo := make([]*pb.FieldIndexInfo, 0, len(col.FieldIndexes))
	var dropIdxID typeutil.UniqueID
	for i, info := range col.FieldIndexes {
		if info.FiledID != fieldSch.FieldID {
			fieldIdxInfo = append(fieldIdxInfo, info)
			continue
		}
		idxMeta, ok := mt.indexID2Meta[info.IndexID]
		if !ok {
			fieldIdxInfo = append(fieldIdxInfo, info)
			log.Warn("index id not has meta", zap.Int64("index id", info.IndexID))
			continue
		}
		if idxMeta.IndexName != indexName {
			fieldIdxInfo = append(fieldIdxInfo, info)
			continue
		}
		dropIdxID = info.IndexID
		fieldIdxInfo = append(fieldIdxInfo, col.FieldIndexes[i+1:]...)
		break
	}

	if len(fieldIdxInfo) == len(col.FieldIndexes) {
		log.Warn("drop index,index not found", zap.String("collection name", collName), zap.String("filed name", fieldName), zap.String("index name", indexName))
		return 0, false, nil
	}

	// update cache
	col.FieldIndexes = fieldIdxInfo
	mt.collID2Meta[collID] = col

	delete(mt.indexID2Meta, dropIdxID)
	for _, part := range col.Partitions {
		if segIDMap, ok := mt.partID2SegID[part.PartitionID]; ok {
			for segID := range segIDMap {
				if segIndexInfos, ok := mt.segID2IndexMeta[segID]; ok {
					delete(segIndexInfos, dropIdxID)
				}
			}
		}
	}

	// update metastore
	err = mt.catalog.DropIndex(mt.ctx, &col, dropIdxID, 0)
	if err != nil {
		return 0, false, err
	}

	return dropIdxID, true, nil
}

// GetSegmentIndexInfoByID return segment index info by segment id
func (mt *MetaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, fieldID int64, idxName string) (pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segIdxMap, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return pb.SegmentIndexInfo{
			SegmentID:   segID,
			FieldID:     fieldID,
			IndexID:     0,
			BuildID:     0,
			EnableIndex: false,
		}, nil
	}
	if len(segIdxMap) == 0 {
		return pb.SegmentIndexInfo{}, fmt.Errorf("segment id %d not has any index", segID)
	}

	if fieldID == -1 && idxName == "" { // return default index
		for _, seg := range segIdxMap {
			info, ok := mt.indexID2Meta[seg.IndexID]
			if ok && info.IndexName == Params.CommonCfg.DefaultIndexName {
				return seg, nil
			}
		}
	} else {
		for idxID, seg := range segIdxMap {
			idxMeta, ok := mt.indexID2Meta[idxID]
			if ok {
				if idxMeta.IndexName != idxName {
					continue
				}
				if seg.FieldID != fieldID {
					continue
				}
				return seg, nil
			}
		}
	}
	return pb.SegmentIndexInfo{}, fmt.Errorf("can't find index name = %s on segment = %d, with filed id = %d", idxName, segID, fieldID)
}

func (mt *MetaTable) GetSegmentIndexInfos(segID typeutil.UniqueID) (map[typeutil.UniqueID]pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	ret, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return nil, fmt.Errorf("segment not found in meta, segment: %d", segID)
	}

	return ret, nil
}

// GetFieldSchema return field schema
func (mt *MetaTable) GetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	return mt.unlockGetFieldSchema(collName, fieldName)
}

func (mt *MetaTable) unlockGetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
		}
	}
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
	}

	for _, field := range col.Fields {
		if field.Name == fieldName {
			return *field, nil
		}
	}
	return schemapb.FieldSchema{}, fmt.Errorf("collection %s doesn't have filed %s", collName, fieldName)
}

// IsSegmentIndexed check if segment has indexed
func (mt *MetaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	return mt.unlockIsSegmentIndexed(segID, fieldSchema, indexParams)
}

func (mt *MetaTable) unlockIsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	segIdx, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return false
	}
	exist := false
	for idxID, meta := range segIdx {
		if meta.FieldID != fieldSchema.FieldID {
			continue
		}
		idxMeta, ok := mt.indexID2Meta[idxID]
		if !ok {
			continue
		}
		if EqualKeyPairArray(indexParams, idxMeta.IndexParams) {
			exist = true
			break
		}
	}
	return exist
}

// GetNotIndexedSegments return segment ids which have no index
func (mt *MetaTable) GetNotIndexedSegments(collName string, fieldName string, idxInfo *pb.IndexInfo, segIDs []typeutil.UniqueID) ([]typeutil.UniqueID, schemapb.FieldSchema, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	fieldSchema, err := mt.unlockGetFieldSchema(collName, fieldName)
	if err != nil {
		return nil, fieldSchema, err
	}

	//TODO:: check index params for sclar field
	// set default index type for scalar index
	if !typeutil.IsVectorType(fieldSchema.GetDataType()) {
		if fieldSchema.DataType == schemapb.DataType_VarChar {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultStringIndexType}}
		} else {
			idxInfo.IndexParams = []*commonpb.KeyValuePair{{Key: "index_type", Value: DefaultIndexType}}
		}
	}

	if idxInfo.IndexParams == nil {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("index param is nil")
	}
	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return nil, schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
		}
	}
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return nil, schemapb.FieldSchema{}, fmt.Errorf("collection %s not found", collName)
	}

	dupIdx := false
	for _, f := range col.FieldIndexes {
		if info, ok := mt.indexID2Meta[f.IndexID]; ok {
			if info.IndexName == idxInfo.IndexName {
				// the index name must be different for different indexes
				if !EqualKeyPairArray(info.IndexParams, idxInfo.IndexParams) || f.FiledID != fieldSchema.FieldID {
					return nil, schemapb.FieldSchema{}, fmt.Errorf("index name(%s) has been exist in collectio(%s), field(%s)", info.IndexName, collName, fieldName)
				}

				// same index name, index params, and fieldId
				dupIdx = true
			}
		}
	}

	// if no same index exist, save new index info to etcd
	if !dupIdx {
		idx := &pb.FieldIndexInfo{
			FiledID: fieldSchema.FieldID,
			IndexID: idxInfo.IndexID,
		}
		col.FieldIndexes = append(col.FieldIndexes, idx)
		k1 := path.Join(CollectionMetaPrefix, strconv.FormatInt(col.CollectionID, 10))
		v1, err := proto.Marshal(model.ConvertToCollectionPB(&col))
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal collMeta fail",
				zap.String("key", k1), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("metaTable GetNotIndexedSegments Marshal collMeta fail key:%s, err:%w", k1, err)
		}

		k2 := path.Join(IndexMetaPrefix, strconv.FormatInt(idx.IndexID, 10))
		v2, err := proto.Marshal(idxInfo)
		if err != nil {
			log.Error("MetaTable GetNotIndexedSegments Marshal idxInfo fail",
				zap.String("key", k2), zap.Error(err))
			return nil, schemapb.FieldSchema{}, fmt.Errorf("metaTable GetNotIndexedSegments Marshal idxInfo fail key:%s, err:%w", k2, err)
		}
		meta := map[string]string{k1: string(v1), k2: string(v2)}

		err = mt.txn.MultiSave(meta)
		if err != nil {
			log.Error("TxnKV MultiSave fail", zap.Error(err))
			panic("TxnKV MultiSave fail")
		}

		mt.collID2Meta[col.CollectionID] = col
		mt.indexID2Meta[idx.IndexID] = *idxInfo
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, segID := range segIDs {
		if exist := mt.unlockIsSegmentIndexed(segID, &fieldSchema, idxInfo.IndexParams); !exist {
			rstID = append(rstID, segID)
		}
	}
	return rstID, fieldSchema, nil
}

// GetIndexByName return index info by index name
func (mt *MetaTable) GetIndexByName(collName, indexName string) (model.Collection, []pb.IndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		collID, ok = mt.collAlias2ID[collName]
		if !ok {
			return model.Collection{}, nil, fmt.Errorf("collection %s not found", collName)
		}
	}
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return model.Collection{}, nil, fmt.Errorf("collection %s not found", collName)
	}

	rstIndex := make([]pb.IndexInfo, 0, len(col.FieldIndexes))
	for _, idx := range col.FieldIndexes {
		idxInfo, ok := mt.indexID2Meta[idx.IndexID]
		if !ok {
			return model.Collection{}, nil, fmt.Errorf("index id = %d not found", idx.IndexID)
		}
		if indexName == "" || idxInfo.IndexName == indexName {
			rstIndex = append(rstIndex, idxInfo)
		}
	}
	return col, rstIndex, nil
}

// GetIndexByID return index info by index id
func (mt *MetaTable) GetIndexByID(indexID typeutil.UniqueID) (*pb.IndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	indexInfo, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, fmt.Errorf("cannot find index, id = %d", indexID)
	}
	return &indexInfo, nil
}

func (mt *MetaTable) dupMeta() (
	map[typeutil.UniqueID]model.Collection,
	map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo,
	map[typeutil.UniqueID]pb.IndexInfo,
) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID2Meta := map[typeutil.UniqueID]model.Collection{}
	segID2IndexMeta := map[typeutil.UniqueID]map[typeutil.UniqueID]pb.SegmentIndexInfo{}
	indexID2Meta := map[typeutil.UniqueID]pb.IndexInfo{}
	for k, v := range mt.collID2Meta {
		collID2Meta[k] = v
	}
	for k, v := range mt.segID2IndexMeta {
		segID2IndexMeta[k] = map[typeutil.UniqueID]pb.SegmentIndexInfo{}
		for k2, v2 := range v {
			segID2IndexMeta[k][k2] = v2
		}
	}
	for k, v := range mt.indexID2Meta {
		indexID2Meta[k] = v
	}
	return collID2Meta, segID2IndexMeta, indexID2Meta
}

// AddAlias add collection alias
func (mt *MetaTable) AddAlias(collectionAlias string, collectionName string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.collAlias2ID[collectionAlias]; ok {
		return fmt.Errorf("duplicate collection alias, alias = %s", collectionAlias)
	}

	if _, ok := mt.collName2ID[collectionAlias]; ok {
		return fmt.Errorf("collection alias collides with existing collection name. collection = %s, alias = %s", collectionAlias, collectionAlias)
	}

	id, ok := mt.collName2ID[collectionName]
	if !ok {
		return fmt.Errorf("aliased collection name does not exist, name = %s", collectionName)
	}
	mt.collAlias2ID[collectionAlias] = id

	coll := &model.Collection{
		CollectionID: id,
		Aliases:      []string{collectionAlias},
	}
	return mt.catalog.CreateAlias(mt.ctx, coll, ts)
}

// DropAlias drop collection alias
func (mt *MetaTable) DropAlias(collectionAlias string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	collectionID, ok := mt.collAlias2ID[collectionAlias]
	if !ok {
		return fmt.Errorf("alias does not exist, alias = %s", collectionAlias)
	}
	delete(mt.collAlias2ID, collectionAlias)

	return mt.catalog.DropAlias(mt.ctx, collectionID, collectionAlias, ts)
}

// AlterAlias alter collection alias
func (mt *MetaTable) AlterAlias(collectionAlias string, collectionName string, ts typeutil.Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if _, ok := mt.collAlias2ID[collectionAlias]; !ok {
		return fmt.Errorf("alias does not exist, alias = %s", collectionAlias)
	}

	id, ok := mt.collName2ID[collectionName]
	if !ok {
		return fmt.Errorf("aliased collection name does not exist, name = %s", collectionName)
	}
	mt.collAlias2ID[collectionAlias] = id

	coll := &model.Collection{
		CollectionID: id,
		Aliases:      []string{collectionAlias},
	}
	return mt.catalog.AlterAlias(mt.ctx, coll, ts)
}

// IsAlias returns true if specific `collectionAlias` is an alias of collection.
func (mt *MetaTable) IsAlias(collectionAlias string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, ok := mt.collAlias2ID[collectionAlias]
	return ok
}

// AddCredential add credential
func (mt *MetaTable) AddCredential(credInfo *internalpb.CredentialInfo) error {
	if credInfo.Username == "" {
		return fmt.Errorf("username is empty")
	}

	credential := &model.Credential{
		Username:          credInfo.Username,
		EncryptedPassword: credInfo.EncryptedPassword,
	}
	return mt.catalog.CreateCredential(mt.ctx, credential)
}

// GetCredential get credential by username
func (mt *MetaTable) getCredential(username string) (*internalpb.CredentialInfo, error) {
	credential, err := mt.catalog.GetCredential(mt.ctx, username)
	return model.ConvertToCredentialPB(credential), err
}

// DeleteCredential delete credential
func (mt *MetaTable) DeleteCredential(username string) error {
	return mt.catalog.DropCredential(mt.ctx, username)
}

// ListCredentialUsernames list credential usernames
func (mt *MetaTable) ListCredentialUsernames() (*milvuspb.ListCredUsersResponse, error) {
	usernames, err := mt.catalog.ListCredentials(mt.ctx)
	if err != nil {
		return nil, fmt.Errorf("list credential usernames err:%w", err)
	}
	return &milvuspb.ListCredUsersResponse{Usernames: usernames}, nil
}
