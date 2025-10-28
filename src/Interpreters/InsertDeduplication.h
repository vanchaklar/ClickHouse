#pragma once

#include <functional>
#include <Processors/Chunk.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageIDMaybeEmpty.h>
#include <Core/Block_fwd.h>

#include <Common/Logger.h>
#include <base/defines.h>


namespace DB
{

class InsertDependenciesBuilder;
using InsertDependenciesBuilderConstPtr = std::shared_ptr<const InsertDependenciesBuilder>;

class DeduplicationInfo : public ChunkInfo, public std::enable_shared_from_this<DeduplicationInfo>
{
protected:
    // InsertDependenciesBuilder::createChainForDeduplicationRetry needs access to private members
    friend class InsertDependenciesBuilder;

    explicit DeduplicationInfo(bool async_insert_);
    DeduplicationInfo(const DeduplicationInfo & other) = default;

public:
    using Ptr = std::shared_ptr<DeduplicationInfo>;

    static Ptr create(bool async_insert_);

    ChunkInfo::Ptr merge(const ChunkInfo::Ptr & right) const override;
    Ptr mergeSelf(const Ptr & right) const;

    ChunkInfo::Ptr clone() const override;
    Ptr cloneSelf() const;


    struct FilterResult
    {
        Block filtered_block;
        Ptr deduplication_info;
        size_t removed_count = 0;
    };
    FilterResult filterSelfDuplicate();

    std::vector<std::string> getBlockIds(const std::string & partition_id) const;

    size_t getCount() const;
    size_t getRows() const;

    std::pair<std::string, size_t> debug(size_t offset) const;
    std::string debug() const;

    // for sync insert: if user token is empty then by_part_writer token would be calculated later by part writer
    // for async insert: if user token is empty then by_data_hash token would be calculated later
    void setUserToken(const String & token, size_t count);
    void setSourceBlockNumber(size_t block_number);
    void setRootViewID(const StorageIDMaybeEmpty & id);

    /// use for provide deduplication hash for the part from one partition
    void setPartWriterHashForPartition(const std::string & hash, size_t count) const;
    /// use for provide deduplication hash for the chunk with maybe multiple partitions in it
    void setPartWriterHashes(const std::vector<std::string> & partitions_hashes, size_t count) const;
    /// hash from part writer would be used as user token for dependent views if no user token has been set before
    void redefineTokensWithDataHash();

    void setViewID(const StorageID & id);
    void setViewBlockNumber(size_t block_number);
    void rememberPartitionChoise(const std::string & partition_id);

    void setInsertDependencies(InsertDependenciesBuilderConstPtr insert_dependencies_);
    void updateOriginalBlock(const Chunk & chunk, SharedHeader header);

    Block deduplicateBlock(const std::vector<std::string> & existing_block_ids, const std::string & partition_id, ContextPtr context);

    const std::string & getLastPartitionChoice() const;
    const std::vector<StorageIDMaybeEmpty> & getVisitedViews() const;

private:
    UInt128 calculateDataHash(size_t offset) const;

    Ptr cloneSelfFilterImpl() const;
    FilterResult filterOriginalBlock(const std::vector<std::string> & collisions, const String & partition_id);
    FilterResult filterImpl(const std::set<size_t> & fitered_offsets, const Block & block);

    Block goRetry(SharedHeader && header, Chunk && filtered_data, Ptr filtered_info, ContextPtr context);

    std::vector<std::string> getHashesForBlocks(Block & block, String partition_id);

    size_t getTokenBegin(size_t pos) const;
    size_t getTokenEnd(size_t pos) const;
    size_t getTokenRows(size_t pos) const;

    void addExtraPart(const String & part);

    std::unordered_map<std::string, std::vector<size_t>> buildBlockIdToOffsetsMap(const std::string & partition_id) const;

    enum Stage
    {
        /// EMPTY -> SOURCE_BLOCK_WITH_USER_TOKEN
        EMPTY,

        /// SOURCE_BLOCK_WITH_USER_TOKEN -> SOURCE_SEQUENCE_NUMBER
        SOURCE_BLOCK_WITH_USER_TOKEN,

        /// SOURCE_SEQUENCE_NUMBER -> SQUASHIGING_SOURCE_BLOCKS
        /// SOURCE_SEQUENCE_NUMBER -> FILTERING_SELF_DUPLICATE
        /// SOURCE_SEQUENCE_NUMBER -> SOURCE_BLOCK_WHITH_PART_HASH
        /// SOURCE_SEQUENCE_NUMBER -> SOURCE_BLOCK_FINALIZED
        SOURCE_SEQUENCE_NUMBER,

        /// SQUASHIGING_SOURCE_BLOCKS -> FILTERING_SELF_DUPLICATE
        /// SQUASHIGING_SOURCE_BLOCKS -> SOURCE_BLOCK_WHITH_PART_HASH
        /// SQUASHIGING_SOURCE_BLOCKS -> SOURCE_BLOCK_FINALIZED
        SQUASHIGING_SOURCE_BLOCKS,

        /// FILTERING_SELF_DUPLICATE -> SOURCE_BLOCK_WHITH_PART_HASH
        /// FILTERING_SELF_DUPLICATE -> SOURCE_BLOCK_FINALIZED
        FILTERING_SELF_DUPLICATE,

        /// SOURCE_BLOCK_WHITH_PART_HASH -> SOURCE_BLOCK_FINALIZED
        SOURCE_BLOCK_WHITH_PART_HASH,

        /// SOURCE_BLOCK_FINALIZED -> TRANSFORM_WITH_VIEW_ID
        SOURCE_BLOCK_FINALIZED,

        /// TRANSFORM_WITH_VIEW_ID -> VIEW_SEQUENCE_NUMBER
        TRANSFORM_WITH_VIEW_ID,

        /// VIEW_SEQUENCE_NUMBER -> SQUASHING_VIEW_BLOCKS
        /// VIEW_SEQUENCE_NUMBER -> SQUASHING_GROUPPED_VIEW_BLOCKS
        /// VIEW_SEQUENCE_NUMBER -> TRANSFORM_WITH_VIEW_ID
        VIEW_SEQUENCE_NUMBER,

        /// SQUASHING_VIEW_BLOCKS -> SQUASHING_GROUPPED_VIEW_BLOCKS
        /// SQUASHING_VIEW_BLOCKS -> TRANSFORM_WITH_VIEW_ID
        SQUASHING_VIEW_BLOCKS,

        /// SQUASHING_GROUPPED_VIEW_BLOCKS -> TRANSFORM_WITH_VIEW_ID
        SQUASHING_GROUPPED_VIEW_BLOCKS,
    };

    const bool is_async_insert = false;

    LoggerPtr logger;
    mutable Stage stage = EMPTY;

    struct TokenDefinition
    {
        // there is a difference how block ids are generated from these two types of tokens
        // if by_part_writer is set then it is used as is
        // if by_user is set then block id is calculated as a hash of this string extended with extra tokens
        // When both are empty then data hash is calculated and used as by_user token
        std::string by_user;
        std::string by_part_writer;

        std::vector<String> extra_tokens;

        static TokenDefinition asUserToken(std::string token);

        void addExtraToken(const String & token);

        void setPartToken(std::string token);
        void resetPartToken();

        using HashCalculator = std::function<UInt128 ()>;
        std::string getBlockId(const std::string & partition_id, HashCalculator get_data_hash);

        bool empty() const;
        bool operator==(const TokenDefinition & other) const;
    };
    mutable std::vector<TokenDefinition> tokens;
    std::vector<size_t> offsets; // points to the last row for each offset

    Block original_block;
    StorageIDMaybeEmpty original_block_view_id;

    std::vector<StorageIDMaybeEmpty> visited_views;
    std::string last_partition_choice;
    InsertDependenciesBuilderConstPtr insert_dependencies;
};

}
