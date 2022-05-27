package kv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Catalog struct {
	Txn      kv.TxnKV
	Snapshot kv.SnapShotKV
}

func (kc *Catalog) CreateCollection(ctx context.Context, coll *model.Collection, ts typeutil.Timestamp) error {
	k1 := fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, coll.CollectionID)
	collInfo := ConvertToCollectionPB(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		log.Error("create collection marshal fail", zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("marshal fail key:%s, err:%w", k1, err)
	}

	kvs := map[string]string{k1: string(v1)}
	// save ddOpStr into etcd
	for k, v := range coll.Extra {
		if k == metastore.DDOperationPrefix {
			ddOpStr, err := metastore.EncodeDdOp(v.(model.DdOperation))
			if err != nil {
				return fmt.Errorf("encodeDdOperation fail, error = %w", err)
			}
			kvs[k] = ddOpStr
		} else {
			kvs[k] = v.(string)
		}
	}

	err = kc.Snapshot.MultiSave(kvs, ts)
	if err != nil {
		log.Error("create collection persist meta fail", zap.String("key", k1), zap.Error(err))
		panic("create collection persist meta fail")
	}

	return nil
}

func (kc *Catalog) CreatePartition(ctx context.Context, coll *model.Collection, partition *model.Partition, ts typeutil.Timestamp) error {
	k1 := fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, coll.CollectionID)
	collInfo := ConvertToCollectionPB(coll)
	v1, err := proto.Marshal(collInfo)
	if err != nil {
		log.Error("create partition marshal fail", zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("marshal fail key:%s, err:%w", k1, err)
	}

	kvs := map[string]string{k1: string(v1)}
	err = kc.Snapshot.MultiSave(kvs, ts)
	if err != nil {
		log.Error("create partition persist meta fail", zap.String("key", k1), zap.Error(err))
		panic("create partition persist meta fail")
	}

	// save ddOpStr into etcd
	ddOpProperties := map[string]string{}
	for k, v := range coll.Extra {
		if k == metastore.DDOperationPrefix {
			ddOpStr, err := metastore.EncodeDdOp(v.(model.DdOperation))
			if err != nil {
				return fmt.Errorf("encodeDdOperation fail, error = %w", err)
			}
			ddOpProperties[k] = ddOpStr
		} else {
			ddOpProperties[k] = v.(string)
		}
	}
	err = kc.Txn.MultiSave(ddOpProperties)
	if err != nil {
		// will not panic, missing create msg
		log.Warn("create partition persist ddop meta fail", zap.Int64("collectionID", coll.CollectionID), zap.Error(err))
	}

	return nil
}

func (kc *Catalog) CreateIndex(ctx context.Context, col *model.Collection, index *model.Index) error {
	k1 := path.Join(metastore.CollectionMetaPrefix, strconv.FormatInt(col.CollectionID, 10))
	v1, err := proto.Marshal(ConvertToCollectionPB(col))
	if err != nil {
		log.Error("create index marshal fail", zap.String("key", k1), zap.Error(err))
		return fmt.Errorf("create index marshal fail key:%s, err:%w", k1, err)
	}

	k2 := path.Join(metastore.IndexMetaPrefix, strconv.FormatInt(index.IndexID, 10))
	v2, err := proto.Marshal(ConvertToIndexPB(index))
	if err != nil {
		log.Error("create index marshal fail", zap.String("key", k2), zap.Error(err))
		return fmt.Errorf("create index marshal fail key:%s, err:%w", k2, err)
	}
	meta := map[string]string{k1: string(v1), k2: string(v2)}

	err = kc.Txn.MultiSave(meta)
	if err != nil {
		log.Error("create index persist meta fail", zap.String("key", k1), zap.Error(err))
		panic("create index persist meta fail")
	}

	return nil
}

func (kc *Catalog) AlterIndex(ctx context.Context, oldIndex *model.Index, newIndex *model.Index) error {
	for _, segmentIndex := range newIndex.SegmentIndexes {
		segment := segmentIndex.Segment
		k := fmt.Sprintf("%s/%d/%d/%d/%d", metastore.SegmentIndexMetaPrefix, newIndex.CollectionID, newIndex.IndexID, segment.PartitionID, segment.SegmentID)
		segIdxInfo := ConvertToSegmentIndexPB(newIndex)
		v, err := proto.Marshal(segIdxInfo)
		if err != nil {
			log.Error("alter index marshal fail", zap.String("key", k), zap.Error(err))
			return fmt.Errorf("alter index marshal fail key:%s, err:%w", k, err)
		}

		err = kc.Txn.Save(k, string(v))
		if err != nil {
			log.Error("alter index persist meta fail", zap.String("key", k), zap.Error(err))
			panic("alter index persist meta fail")
		}
	}

	return nil
}

func (kc *Catalog) AddAlias(ctx context.Context, collection *model.Collection, ts typeutil.Timestamp) error {
	k := fmt.Sprintf("%s/%s", metastore.CollectionAliasMetaPrefix, collection.Aliases[0])
	v, err := proto.Marshal(&pb.CollectionInfo{ID: collection.CollectionID, Schema: &schemapb.CollectionSchema{Name: collection.Aliases[0]}})
	if err != nil {
		log.Error("create alias marshal fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("create alias marshal fail key:%s, err:%w", k, err)
	}

	err = kc.Snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("create alias persist meta fail", zap.String("key", k), zap.Error(err))
		panic("create alias persist meta fail")
	}

	return nil
}

func (kc *Catalog) CreateCredential(ctx context.Context, credential *model.Credential) error {
	k := fmt.Sprintf("%s/%s", metastore.CredentialPrefix, credential.Username)
	v, err := json.Marshal(&internalpb.CredentialInfo{EncryptedPassword: credential.EncryptedPassword})
	if err != nil {
		log.Error("create credential marshal fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("create credential marshal fail key:%s, err:%w", k, err)
	}
	err = kc.Txn.Save(k, string(v))
	if err != nil {
		log.Error("create credential persist meta fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("create credential persist meta fail key:%s, err:%w", credential.Username, err)
	}

	return nil
}

func (kc *Catalog) GetCollectionByID(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*model.Collection, error) {
	collKey := fmt.Sprintf("%s/%d", metastore.CollectionMetaPrefix, collectionID)
	collVal, err := kc.Snapshot.Load(collKey, ts)
	if err != nil {
		log.Error("get collection meta fail", zap.String("key", collKey), zap.Error(err))
		return nil, err
	}
	collMeta := &pb.CollectionInfo{}
	err = proto.Unmarshal([]byte(collVal), collMeta)
	if err != nil {
		log.Error("collection meta marshal fail", zap.String("key", collKey), zap.Error(err))
		return nil, err
	}
	return ConvertCollectionPBToModel(collMeta, nil), nil
}

func (kc *Catalog) CollectionExists(ctx context.Context, collectionID typeutil.UniqueID, ts typeutil.Timestamp) bool {
	_, err := kc.GetCollectionByID(ctx, collectionID, ts)
	return err == nil
}

func (kc *Catalog) GetCredential(ctx context.Context, username string) (*model.Credential, error) {
	k := fmt.Sprintf("%s/%s", metastore.CredentialPrefix, username)
	v, err := kc.Txn.Load(k)
	if err != nil {
		log.Warn("get credential meta fail", zap.String("key", k), zap.Error(err))
		return nil, err
	}

	credentialInfo := internalpb.CredentialInfo{}
	err = json.Unmarshal([]byte(v), &credentialInfo)
	if err != nil {
		return nil, fmt.Errorf("unmarshal credential info err:%w", err)
	}
	return &model.Credential{Username: username, EncryptedPassword: credentialInfo.EncryptedPassword}, nil
}

func (kc *Catalog) DropCollection(ctx context.Context, collectionInfo *model.Collection, ts typeutil.Timestamp) error {
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
		log.Error("drop collection update meta fail", zap.Int64("collectionID", collectionInfo.CollectionID), zap.Error(err))
		panic("drop collection update meta fail")
	}

	// Txn operation
	kvs := map[string]string{}
	for k, v := range collectionInfo.Extra {
		if k == metastore.DDOperationPrefix {
			ddOpStr, err := metastore.EncodeDdOp(v.(model.DdOperation))
			if err != nil {
				return fmt.Errorf("encodeDdOperation fail, error = %w", err)
			}
			kvs[k] = ddOpStr
		} else {
			kvs[k] = v.(string)
		}
	}

	delMetaKeysTxn := []string{
		fmt.Sprintf("%s/%d", metastore.SegmentIndexMetaPrefix, collectionInfo.CollectionID),
		fmt.Sprintf("%s/%d", metastore.IndexMetaPrefix, collectionInfo.CollectionID),
	}

	err = kc.Txn.MultiSaveAndRemoveWithPrefix(kvs, delMetaKeysTxn)
	if err != nil {
		log.Warn("drop collection update meta fail", zap.Int64("collectionID", collectionInfo.CollectionID), zap.Error(err))
	}

	return nil
}

func (kc *Catalog) DropPartition(ctx context.Context, collectionInfo *model.Collection, partitionID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta := ConvertToCollectionPB(collectionInfo)

	k := path.Join(metastore.CollectionMetaPrefix, strconv.FormatInt(collectionInfo.CollectionID, 10))
	v, err := proto.Marshal(collMeta)
	if err != nil {
		log.Error("drop partition marshal fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("drop partition marshal fail key:%s, err:%w", k, err)
	}

	err = kc.Snapshot.Save(k, string(v), ts)
	if err != nil {
		log.Error("drop partition update collection meta fail",
			zap.Int64("collectionID", collectionInfo.CollectionID),
			zap.Int64("partitionID", partitionID),
			zap.Error(err))
		panic("drop partition update collection meta fail")
	}

	var delMetaKeys []string
	for _, idxInfo := range collMeta.FieldIndexes {
		k := fmt.Sprintf("%s/%d/%d/%d", metastore.SegmentIndexMetaPrefix, collMeta.ID, idxInfo.IndexID, partitionID)
		delMetaKeys = append(delMetaKeys, k)
	}

	// Txn operation
	metaTxn := map[string]string{}
	for k, v := range collectionInfo.Extra {
		if k == metastore.DDOperationPrefix {
			ddOpStr, err := metastore.EncodeDdOp(v.(model.DdOperation))
			if err != nil {
				return fmt.Errorf("encodeDdOperation fail, error = %w", err)
			}
			metaTxn[k] = ddOpStr
		} else {
			metaTxn[k] = v.(string)
		}
	}
	err = kc.Txn.MultiSaveAndRemoveWithPrefix(metaTxn, delMetaKeys)
	if err != nil {
		log.Warn("drop partition update meta fail",
			zap.Int64("collectionID", collectionInfo.CollectionID),
			zap.Int64("partitionID", partitionID),
			zap.Error(err))
		// will not panic, failed Txn shall be treated by garbage related logic
	}

	return nil
}

func (kc *Catalog) DropIndex(ctx context.Context, collectionInfo *model.Collection, dropIdxID typeutil.UniqueID, ts typeutil.Timestamp) error {
	collMeta := ConvertToCollectionPB(collectionInfo)

	k := path.Join(metastore.CollectionMetaPrefix, strconv.FormatInt(collectionInfo.CollectionID, 10))
	v, err := proto.Marshal(collMeta)
	if err != nil {
		log.Error("drop index marshal fail",
			zap.String("key", k), zap.Error(err))
		return fmt.Errorf("drop partition marshal fail key:%s, err:%w", k, err)
	}
	saveMeta := map[string]string{k: string(v)}

	delMeta := []string{
		fmt.Sprintf("%s/%d/%d", metastore.SegmentIndexMetaPrefix, collectionInfo.CollectionID, dropIdxID),
		fmt.Sprintf("%s/%d/%d", metastore.IndexMetaPrefix, collectionInfo.CollectionID, dropIdxID),
	}

	err = kc.Txn.MultiSaveAndRemoveWithPrefix(saveMeta, delMeta)
	if err != nil {
		log.Error("drop partition update meta fail",
			zap.Int64("collectionID", collectionInfo.CollectionID),
			zap.Int64("indexID", dropIdxID),
			zap.Error(err))
		panic("drop partition update meta fail")
	}

	return nil
}

func (kc *Catalog) DropCredential(ctx context.Context, username string) error {
	k := fmt.Sprintf("%s/%s", metastore.CredentialPrefix, username)

	err := kc.Txn.Remove(k)
	if err != nil {
		log.Error("drop credential update meta fail", zap.String("key", k), zap.Error(err))
		return fmt.Errorf("drop credential update meta fail key:%s, err:%w", username, err)
	}
	return nil
}

func (kc *Catalog) DropAlias(ctx context.Context, collectionID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	delMetakeys := []string{
		fmt.Sprintf("%s/%s", metastore.CollectionAliasMetaPrefix, alias),
	}

	meta := make(map[string]string)
	err := kc.Snapshot.MultiSaveAndRemoveWithPrefix(meta, delMetakeys, ts)
	if err != nil {
		log.Error("drop alias update meta fail", zap.String("alias", alias), zap.Error(err))
		panic("drop alias update meta fail")
	}

	return nil
}

func (kc *Catalog) GetCollectionByName(ctx context.Context, collectionName string, ts typeutil.Timestamp) (*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(metastore.CollectionMetaPrefix, ts)
	if err != nil {
		log.Warn("get collection meta fail", zap.String("collectionName", collectionName), zap.Error(err))
		return nil, err
	}
	for _, val := range vals {
		colMeta := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(val), &colMeta)
		if err != nil {
			log.Warn("get collection meta unmarshal fail", zap.String("collectionName", collectionName), zap.Error(err))
			continue
		}
		if colMeta.Schema.Name == collectionName {
			return ConvertCollectionPBToModel(&colMeta, nil), nil
		}
	}
	return nil, fmt.Errorf("can't find collection: %s, at timestamp = %d", collectionName, ts)
}

func (kc *Catalog) ListCollections(ctx context.Context, ts typeutil.Timestamp) (map[string]*model.Collection, error) {
	_, vals, err := kc.Snapshot.LoadWithPrefix(metastore.CollectionMetaPrefix, ts)
	if err != nil {
		log.Error("get collections meta fail",
			zap.String("prefix", metastore.CollectionMetaPrefix),
			zap.Uint64("timestamp", ts),
			zap.Error(err))
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
		colls[collMeta.Schema.Name] = ConvertCollectionPBToModel(&collMeta, nil)
	}
	return colls, nil
}

func (kc *Catalog) ListAliases(ctx context.Context) ([]*model.Collection, error) {
	_, values, err := kc.Snapshot.LoadWithPrefix(metastore.CollectionAliasMetaPrefix, 0)
	if err != nil {
		log.Error("get aliases meta fail", zap.String("prefix", metastore.CollectionAliasMetaPrefix), zap.Error(err))
		return nil, err
	}
	var colls []*model.Collection
	for _, value := range values {
		aliasInfo := pb.CollectionInfo{}
		err = proto.Unmarshal([]byte(value), &aliasInfo)
		if err != nil {
			log.Warn("unmarshal aliases failed", zap.Error(err))
			continue
		}
		colls = append(colls, ConvertCollectionPBToModel(&aliasInfo, nil))
	}
	return colls, nil
}

func (kc *Catalog) listSegmentIndexes(ctx context.Context) (map[int64]*model.Index, error) {
	_, values, err := kc.Txn.LoadWithPrefix(metastore.SegmentIndexMetaPrefix)
	if err != nil {
		log.Error("list segment index meta fail", zap.String("prefix", metastore.SegmentIndexMetaPrefix), zap.Error(err))
		return nil, err
	}
	indexes := make(map[int64]*model.Index, len(values))
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

		index := ConvertSegmentIndexPBToModel(&segmentIndexInfo)
		if _, ok := indexes[segmentIndexInfo.IndexID]; ok {
			log.Warn("duplicated index id exists in segment index meta", zap.Int64("index id", segmentIndexInfo.IndexID))
		}

		indexes[segmentIndexInfo.IndexID] = index
	}

	return indexes, nil
}

func (kc *Catalog) listIndexMeta(ctx context.Context) (map[int64]*model.Index, error) {
	_, values, err := kc.Txn.LoadWithPrefix(metastore.IndexMetaPrefix)
	if err != nil {
		log.Error("list index meta fail", zap.String("prefix", metastore.IndexMetaPrefix), zap.Error(err))
		return nil, err
	}
	indexes := make(map[int64]*model.Index, len(values))
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

		index := ConvertIndexPBToModel(&meta)
		if _, ok := indexes[meta.IndexID]; ok {
			log.Warn("duplicated index id exists in index meta", zap.Int64("index id", meta.IndexID))
		}

		indexes[meta.IndexID] = index
	}
	return indexes, nil
}

func (kc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	indexMeta, err := kc.listIndexMeta(ctx)
	if err != nil {
		return nil, err
	}

	segmentIndexMeta, err := kc.listSegmentIndexes(ctx)
	if err != nil {
		return nil, err
	}

	var indexes []*model.Index
	//merge index and segment index
	for indexID, index := range indexMeta {
		segmentIndex, ok := segmentIndexMeta[indexID]
		if ok {
			index = MergeIndexModel(index, segmentIndex)
			delete(segmentIndexMeta, indexID)
		}
		indexes = append(indexes, index)
	}

	// add remain segmentIndexMeta
	for _, index := range segmentIndexMeta {
		indexes = append(indexes, index)
	}

	return indexes, nil
}

func (kc *Catalog) ListCredentials(ctx context.Context) ([]string, error) {
	keys, _, err := kc.Txn.LoadWithPrefix(metastore.CredentialPrefix)
	if err != nil {
		log.Error("list all credential usernames fail", zap.String("prefix", metastore.CredentialPrefix), zap.Error(err))
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

func (kc *Catalog) UpdateDDOperation(ctx context.Context, ddOp model.DdOperation, isSent bool) error {
	return kc.Txn.Save(metastore.DDMsgSendPrefix, strconv.FormatBool(true))
}

func (kc *Catalog) IsDDMsgSent(ctx context.Context) (bool, error) {
	value, err := kc.Txn.Load(metastore.DDMsgSendPrefix)
	if err != nil {
		log.Error("load dd-msg-send from kv failed", zap.Error(err))
		return true, err
	}
	flag, err := strconv.ParseBool(value)
	if err != nil {
		log.Error("invalid value %s", zap.String("is_sent", value))
		return true, err
	}
	return flag, nil
}

func (kc *Catalog) LoadDdOperation(ctx context.Context) (model.DdOperation, error) {
	ddOpStr, err := kc.Txn.Load(metastore.DDOperationPrefix)
	if err != nil {
		log.Error("load dd-operation from kv failed", zap.Error(err))
		return model.DdOperation{}, err
	}
	var ddOp metastore.DdOperation
	if err = json.Unmarshal([]byte(ddOpStr), &ddOp); err != nil {
		log.Error("unmarshal dd operation failed", zap.Error(err))
		return model.DdOperation{}, err
	}
	return ConvertDdOperationToModel(ddOp), nil
}

func (kc *Catalog) Close() {
	panic("implement me")
}
