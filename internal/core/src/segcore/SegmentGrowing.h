// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <vector>

#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "query/Plan.h"
#include "query/deprecated/GeneralQuery.h"
#include "segcore/SegmentInterface.h"

namespace milvus::segcore {

using SearchResult = milvus::SearchResult;
struct RowBasedRawData {
    void* raw_data;      // schema
    int sizeof_per_row;  // alignment
    int64_t count;
};

struct ColumnBasedRawData {
    std::vector<aligned_vector<uint8_t>> columns_;
    int64_t count;
};

class SegmentGrowing : public SegmentInternalInterface {
 public:
    virtual void
    disable_small_index() = 0;

    virtual int64_t
    PreInsert(int64_t size) = 0;

    virtual void
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const InsertData* insert_data) = 0;

    // virtual int64_t
    // PreDelete(int64_t size) = 0;

    // virtual Status
    // Delete(int64_t reserved_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) = 0;

 public:
    virtual ssize_t
    get_deleted_count() const = 0;
};

using SegmentGrowingPtr = std::unique_ptr<SegmentGrowing>;

}  // namespace milvus::segcore
