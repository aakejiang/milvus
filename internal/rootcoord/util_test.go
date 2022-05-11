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
	"testing"

	"github.com/milvus-io/milvus/internal/metastore"

	"github.com/milvus-io/milvus/internal/metastore/model"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
)

func Test_EqualKeyPairArray(t *testing.T) {
	p1 := []*commonpb.KeyValuePair{
		{
			Key:   "k1",
			Value: "v1",
		},
	}

	p2 := []*commonpb.KeyValuePair{}
	assert.False(t, metastore.EqualKeyPairArray(p1, p2))

	p2 = append(p2, &commonpb.KeyValuePair{
		Key:   "k2",
		Value: "v2",
	})
	assert.False(t, metastore.EqualKeyPairArray(p1, p2))
	p2 = []*commonpb.KeyValuePair{
		{
			Key:   "k1",
			Value: "v2",
		},
	}
	assert.False(t, metastore.EqualKeyPairArray(p1, p2))

	p2 = []*commonpb.KeyValuePair{
		{
			Key:   "k1",
			Value: "v1",
		},
	}
	assert.True(t, metastore.EqualKeyPairArray(p1, p2))
}

func Test_GetFieldSchemaByID(t *testing.T) {
	coll := &model.Collection{
		Fields: []*model.Field{
			{
				FieldID: 1,
			},
		},
	}
	_, err := metastore.GetFieldSchemaByID(coll, 1)
	assert.Nil(t, err)
	_, err = metastore.GetFieldSchemaByID(coll, 2)
	assert.NotNil(t, err)
}

func Test_GetFieldSchemaByIndexID(t *testing.T) {
	coll := &model.Collection{
		Fields: []*model.Field{
			{
				FieldID: 1,
			},
		},
		FieldIndexes: []*model.Index{
			{
				FieldID: 1,
				IndexID: 2,
			},
		},
	}
	_, err := metastore.GetFieldSchemaByIndexID(coll, 2)
	assert.Nil(t, err)
	_, err = metastore.GetFieldSchemaByIndexID(coll, 3)
	assert.NotNil(t, err)
}

func Test_EncodeMsgPositions(t *testing.T) {
	mp := &msgstream.MsgPosition{
		ChannelName: "test",
		MsgID:       []byte{1, 2, 3},
	}

	str, err := metastore.EncodeMsgPositions([]*msgstream.MsgPosition{})
	assert.Empty(t, str)
	assert.Nil(t, err)

	mps := []*msgstream.MsgPosition{mp}
	str, err = metastore.EncodeMsgPositions(mps)
	assert.NotEmpty(t, str)
	assert.Nil(t, err)
}

func Test_DecodeMsgPositions(t *testing.T) {
	mp := &msgstream.MsgPosition{
		ChannelName: "test",
		MsgID:       []byte{1, 2, 3},
	}

	str, err := metastore.EncodeMsgPositions([]*msgstream.MsgPosition{mp})
	assert.Nil(t, err)

	mpOut := make([]*msgstream.MsgPosition, 1)
	err = metastore.DecodeMsgPositions(str, &mpOut)
	assert.Nil(t, err)

	err = metastore.DecodeMsgPositions("", &mpOut)
	assert.Nil(t, err)

	err = metastore.DecodeMsgPositions("null", &mpOut)
	assert.Nil(t, err)
}
