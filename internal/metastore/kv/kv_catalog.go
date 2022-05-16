package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"

	"github.com/milvus-io/milvus/internal/metastore"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type KVCatalog struct {
	Txn      kv.TxnKV
	Snapshot kv.SnapShotKV
}

func (kc *KVCatalog) CreateCollection(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	k1 := fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, coll.CollectionID)
	collInfo := ConvertToCollectionPB(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		log.Error("marshal fail", zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("marshal fail key:%s, err:%w", k1, err)
	}

	// save ddOpStr into etcd
	kvs := map[string]string{k1: string(v1)}
	for k, v := range coll.Extra {
		kvs[k] = v
	}

	err = kc.Snapshot.MultiSave(kvs, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSave fail", zap.Error(err))
		panic("SnapShotKV MultiSave fail")
	}

	return nil
}

func (kc *KVCatalog) CreatePartition(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	k1 := fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, coll.CollectionID)
	collInfo := ConvertToCollectionPB(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		log.Error("marshal fail", zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("marshal fail key:%s, err:%w", k1, err)
	}

	kvs := map[string]string{k1: string(v1)}
	err = kc.Snapshot.MultiSave(kvs, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSave fail", zap.Error(err))
		panic("SnapShotKV MultiSave fail")
	}

	// save ddOpStr into etcd
	err = kc.Txn.MultiSave(coll.Extra)
	if err != nil {
		// will not panic, missing create msg
		log.Warn("TxnKV MultiSave fail", zap.Error(err))
	}

	return nil
}

func (kc *KVCatalog) CreateIndex(ctx context.Context, segIndex *model.SegmentIndex) error {
	k := fmt.Sprintf("%s/%d/%d/%d/%d", metastore.SegmentIndexMetaPrefix, segIndex.CollectionID, segIndex.IndexID, segIndex.PartitionID, segIndex.SegmentID)
	segIdxInfo := ConvertToSegmentIndexPB(segIndex)
	v, err := proto.Marshal(segIdxInfo)
	if err != nil {
		log.Error("marshal segIdxInfo fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("marshal segIdxInfo fail key:%s, err:%w", k, err)
	}

	err = kc.Txn.Save(k, string(v))
	if err != nil {
		log.Error("TxnKV Save fail", zap.Error(err))
		panic("TxnKV Save fail")
	}

	return nil
}

func (kc *KVCatalog) CreateAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	k := fmt.Sprintf("%s/%s", metastore.CollectionAliasMetaPrefix, collection.Aliases[0])
	v, err := proto.Marshal(&pb.CollectionInfo{ID: collection.CollectionID, Schema: &schemapb.CollectionSchema{Name: collection.Aliases[0]}})
	if err != nil {
		log.Error("marshal CollectionInfo fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("marshal CollectionInfo fail key:%s, err:%w", k, err)
	}

	err = kc.Snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV Save fail", zap.Error(err))
		panic("SnapShotKV Save fail")
	}

	return nil
}

func (kc *KVCatalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	k := fmt.Sprintf("%s/%s", metastore.CredentialPrefix, credential.Username)
	v, err := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: credential.EncryptedPassword})
	if err != nil {
		log.Error("marshal credential info fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("marshal credential info fail key:%s, err:%w", k, err)
	}
	err = kc.Txn.Save(k, string(v))
	if err != nil {
		log.Error("TxnKV save fail", zap.Error(err))
		return fmt.Errorf("TxnKV save fail key:%s, err:%w", credential.Username, err)
	}

	return nil
}

func (kc *KVCatalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	collKey := fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, collectionID)
	collVal, err := kc.Snapshot.Load(collKey, ts)
	if err != nil {
		log.Error("SnapShotKV Load fail", zap.Error(err))
		return nil, err
	}
	collMeta := &pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), collMeta)
	if err != nil {
		log.Error("unmarshal collection info fail", zap.String("key", collKey), zap.Error(err))
		return nil, err
	}
	return ConvertCollectionPBToModel(collMeta, map[string]string{}), nil
}

func (kc *KVCatalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	_, err := kc.GetCollectionByID(ctx, collectionID, ts)
	return err == nil
}

func (kc *KVCatalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	k := fmt.Sprintf("%s/%s", metastore.CredentialPrefix, username)
	v, err := kc.Txn.Load(k)
	if err != nil {
		log.Warn("TxnKV load fail", zap.String("key", k), zap.Error(err))
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, fmt.Errorf("unmarshal credential info err:%w", err)
	}
	return &model.Credential{Username: username, EncryptedPassword: credentialInfo.EncryptedPassword}, nil
}

func (kc *KVCatalog) AlterAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	return kc.CreateAlias(ctx, collection, ts)
}

func (kc *KVCatalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
	delMetakeysSnap := []string{
		fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, collectionInfo.CollectionID),
	}
	for _, alias := range collectionInfo.Aliases {
		delMetakeysSnap = append(delMetakeysSnap,
			fmt.Sprintf("%s/%s", metastore.CollectionAliasMetaPrefix, alias),
		)
	}

	err := kc.Snapshot.MultiSaveAndRemoveWithPrefix(map[string]string{}, delMetakeysSnap, ts)
	if err != nil {
		log.Error("SnapshotKV save and remove failed", zap.Error(err))
		panic("save etcd failed")
	}

	// Txn operation
	kvs := map[string]string{}
	for k, v := range collectionInfo.Extra {
		kvs[k] = v
	}

	delMetaKeysTxn := []string{
		fmt.Sprintf("%s/%d", metastore.SegmentIndexMetaPrefix, collectionInfo.CollectionID),
		fmt.Sprintf("%s/%d", metastore.IndexMetaPrefix, collectionInfo.CollectionID),
	}

	err = kc.Txn.MultiSaveAndRemoveWithPrefix(kvs, delMetaKeysTxn)
	if err != nil {
		log.Warn("TxnKV save and remove failed", zap.Error(err))
	}

	return nil
}

func (kc *KVCatalog) DropPartition(ctx context.Context, collectionInfo *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta := ConvertToCollectionPB(collectionInfo)

	k := path.Join(metastore.CollectionMetaPrefix, strconv.FormatInt(collectionInfo.CollectionID, 10))
	v, err := proto.Marshal(collMeta)
	if err != nil {
		log.Error("MetaTable DeletePartition Marshal collectionMeta fail",
			zap.String("key", k), zap.Error(err))
		return fmt.Errorf("metaTable DeletePartition Marshal collectionMeta fail key:%s, err:%w", k, err)
	}

	err = kc.Snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}

	var delMetaKeys []string
	for _, idxInfo := range collMeta.FieldIndexes {
		k := fmt.Sprintf("%s/%d/%d/%d", metastore.SegmentIndexMetaPrefix, collMeta.ID, idxInfo.IndexID, partitionID)
		delMetaKeys = append(delMetaKeys, k)
	}

	// Txn operation
	metaTxn := map[string]string{}
	for k, v := range collectionInfo.Extra {
		metaTxn[k] = v
	}
	err = kc.Txn.MultiSaveAndRemoveWithPrefix(metaTxn, delMetaKeys)
	if err != nil {
		log.Warn("TxnKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		// will not panic, failed Txn shall be treated by garbage related logic
	}

	return nil
}

func (kc *KVCatalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta := ConvertToCollectionPB(collectionInfo)

	k := path.Join(metastore.CollectionMetaPrefix, strconv.FormatInt(collectionInfo.CollectionID, 10))
	v, err := proto.Marshal(collMeta)
	if err != nil {
		log.Error("MetaTable DropIndex Marshal collMeta fail",
			zap.String("key", k), zap.Error(err))
		return fmt.Errorf("metaTable DropIndex Marshal collMeta fail key:%s, err:%w", k, err)
	}
	saveMeta := map[string]string{k: string(v)}

	delMeta := []string{
		fmt.Sprintf("%s/%d/%d", metastore.SegmentIndexMetaPrefix, collectionInfo.CollectionID, dropIdxID),
		fmt.Sprintf("%s/%d/%d", metastore.IndexMetaPrefix, collectionInfo.CollectionID, dropIdxID),
	}

	err = kc.Txn.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta)
	if err != nil {
		log.Error("TxnKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("TxnKV MultiSaveAndRemoveWithPrefix fail")
	}

	return nil
}

func (kc *KVCatalog) DropCredential(ctx context.Context, username string) error {
	k := fmt.Sprintf("%s/%s", metastore.CredentialPrefix, username)

	err := kc.Txn.Remove(k)
	if err != nil {
		log.Error("MetaTable remove fail", zap.Error(err))
		return fmt.Errorf("remove credential fail key:%s, err:%w", username, err)
	}
	return nil
}

func (kc *KVCatalog) DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	delMetakeys := []string{
		fmt.Sprintf("%s/%s", metastore.CollectionAliasMetaPrefix, alias),
	}

	meta := make(map[string]string)
	err := kc.Snapshot.MultiSaveAndRemoveWithPrefix(meta, delMetakeys, ts)
	if err != nil {
		log.Error("SnapShotKV MultiSaveAndRemoveWithPrefix fail", zap.Error(err))
		panic("SnapShotKV MultiSaveAndRemoveWithPrefix fail")
	}

	return nil
}

func (kc *KVCatalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(metastore.CollectionMetaPrefix, ts)
	if err != nil {
		log.Warn("failed to load table from meta Snapshot", zap.Error(err))
		return nil, err
	}
	for _, val := range vals {
		colMeta := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(val), &colMeta)
		if err != nil {
			log.Warn("unmarshal collection info failed", zap.Error(err))
			continue
		}
		if colMeta.Schema.Name == collectionName {
			return ConvertCollectionPBToModel(&colMeta, map[string]string{}), nil
		}
	}
	return nil, fmt.Errorf("can't find collection: %s, at timestamp = %d", collectionName, ts)
}

func (kc *KVCatalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(metastore.CollectionMetaPrefix, ts)
	if err != nil {
		log.Error("load with prefix error", zap.Uint64("timestamp", ts), zap.Error(err))
		return nil, nil
	}
	colls := make(map[string]*model.Collection)
	for _, val := range vals {
		collMeta := pb.CollectionInfo{}
		err := proto.Unmarshal([]byte(val), &collMeta)
		if err != nil {
			log.Warn("unmarshal collection info failed", zap.Error(err))
			continue
		}
		colls[collMeta.Schema.Name] = ConvertCollectionPBToModel(&collMeta, map[string]string{})
	}
	return colls, nil
}

func (kc *KVCatalog) ListAliases(ctx context.Context) ([]*model.Collection, error) {
	_, values, err := kc.Snapshot.LoadWithPrefix(metastore.CollectionAliasMetaPrefix, 0)
	if err != nil {
		log.Error("load with prefix error", zap.Error(err))
		return nil, err
	}
	var colls []*model.Collection
	for _, value := range values {
		aliasInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &aliasInfo)
		if err != nil {
			log.Warn("unmarshal collection info failed", zap.Error(err))
			continue
		}
		colls = append(colls, ConvertCollectionPBToModel(&aliasInfo, map[string]string{}))
	}
	return colls, nil
}

func (kc *KVCatalog) ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error) {
	_, values, err := kc.Txn.LoadWithPrefix(metastore.SegmentIndexMetaPrefix)
	if err != nil {
		log.Error("load with prefix error", zap.Error(err))
		return nil, err
	}
	var indexes []*model.SegmentIndex
	for _, value := range values {
		if bytes.Equal([]byte(value), SuffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.Unmarshal([]byte(value), &segmentIndexInfo)
		if err != nil {
			log.Warn("unmarshal segment index info failed", zap.Error(err))
			continue
		}
		indexes = append(indexes, ConvertSegmentIndexPBToModel(&segmentIndexInfo))
	}
	return indexes, nil
}

func (kc *KVCatalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	_, values, err := kc.Txn.LoadWithPrefix(metastore.IndexMetaPrefix)
	if err != nil {
		log.Error("load with prefix error", zap.Error(err))
		return nil, err
	}
	var indexes []*model.Index
	for _, value := range values {
		if bytes.Equal([]byte(value), SuffixSnapshotTombstone) {
			// backward compatibility, IndexMeta used to be in SnapshotKV
			continue
		}
		meta := pb.IndexInfo{}
		err = proto.Unmarshal([]byte(value), &meta)
		if err != nil {
			log.Warn("unmarshal index info failed", zap.Error(err))
			continue
		}
		indexes = append(indexes, ConvertIndexPBToModel(&meta))
	}
	return indexes, nil
}

func (kc *KVCatalog) ListCredentials(ctx context.Context) ([]string, error) {
	keys, _, err := kc.Txn.LoadWithPrefix(metastore.CredentialPrefix)
	if err != nil {
		log.Error("MetaTable list all credential usernames fail", zap.Error(err))
		return nil, err
	}

	var usernames []string
	for _, path := range keys {
		username := typeutil.After(path, metastore.UserSubPrefix+"/")
		if len(username) == 0 {
			log.Warn("no username extract from path:", zap.String("path", path))
			continue
		}
		usernames = append(usernames, username)
	}
	return usernames, nil
}

func (kc *KVCatalog) Close() {
	panic("implement me")
}
