package block

// deduplicationLevelSharded and deduplicationLevelUnsharded are the compaction
// levels at or above which a block's contents are fully deduplicated.
//
// With split-and-merge sharding enabled, level 2 is the intermediate split
// stage: each group is split into split_shards parts which are not yet
// deduplicated, so the first authoritative level is 3. Without sharding,
// compaction deduplicates in a single step and level 2 is authoritative.
const (
	deduplicationLevelUnsharded int32 = 2
	deduplicationLevelSharded   int32 = 3
)

// DeduplicationLevel returns the compaction level at or above which a block is
// authoritative for its time range: its contents are deduplicated and it fully
// replaces its ancestors.
//
// Both the compactor and the querier must agree on this value. The querier uses
// it to decide whether a block can be served as-is or has to be merged with
// deduplication; the compactor uses it to avoid leaving blocks in a state the
// querier will not serve.
func DeduplicationLevel(sharded bool) int32 {
	if sharded {
		return deduplicationLevelSharded
	}
	return deduplicationLevelUnsharded
}

// IsIntermediate reports whether a block at the given compaction level is an
// intermediate compaction artefact rather than an authoritative block, i.e. it
// sits below the deduplication level.
//
// Intermediate blocks are still readable, but they may overlap with other
// blocks, so a query that includes one must deduplicate.
func IsIntermediate(level int32, sharded bool) bool {
	return level < DeduplicationLevel(sharded)
}
