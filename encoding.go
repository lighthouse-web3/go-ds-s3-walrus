package walrusds

// Walrus bills storage by a blob's *encoded* size, not its raw size: RedStuff
// erasure coding expands the data (~4.5–5x) and adds a fixed per-blob metadata
// overhead (~64 MiB on a 1000-shard committee) for the sliver Merkle roots and
// blob ID stored on every shard. The encoded size is what the publisher reports
// as storage.storageSize / resourceOperation.encodedLength and what WAL cost is
// linear in. This file reproduces that figure locally so we can fill it in for
// responses that omit it (e.g. already-certified/deduplicated blobs) and so
// operators can estimate datacap before upload.

// DefaultNShards is the Walrus mainnet committee shard count. The encoded
// (billed) size of a blob depends on its unencoded size and this shard count.
const DefaultNShards = 1000

// storageUnitBytes is the granularity Walrus sells storage in: WAL cost is
// charged per whole 1 MiB unit of encoded size per epoch.
const storageUnitBytes = 1 << 20

// EncodedBlobLength returns the Walrus *encoded* size in bytes of a blob whose
// unencoded length is unencodedLength, for a committee of nShards shards using
// the RS2 (Reed-Solomon) encoding used on Walrus mainnet. This is the figure
// Walrus uses for WAL storage billing ("datacap").
//
// It mirrors MystenLabs' reference implementation exactly (RS2 decoding safety
// limit = 0): for a 17-byte blob on 1000 shards it returns 66,034,000, matching
// the value documented for the publisher's storage.storageSize. A value <= 0
// for nShards falls back to DefaultNShards.
func EncodedBlobLength(unencodedLength int64, nShards int) int64 {
	if nShards <= 0 {
		nShards = DefaultNShards
	}
	n := int64(nShards)

	// RS2 source-symbol counts, derived from the Byzantine fault bound.
	maxFaulty := (n - 1) / 3
	minCorrect := n - maxFaulty
	primarySymbols := minCorrect - maxFaulty // == n - 2*maxFaulty
	secondarySymbols := minCorrect           // == n - maxFaulty
	if primarySymbols <= 0 || secondarySymbols <= 0 {
		return 0
	}

	u := unencodedLength
	if u < 1 {
		u = 1
	}
	symbolSize := (u-1)/(primarySymbols*secondarySymbols) + 1
	if symbolSize%2 == 1 { // RS2 requires an even symbol size
		symbolSize++
	}
	sliverSize := (primarySymbols + secondarySymbols) * symbolSize * n

	// Per-shard metadata: a 32 B Merkle root for each shard's primary and
	// secondary sliver, plus the 32 B blob ID, replicated to every shard.
	const digestLen = 32
	const blobIDLen = 32
	metadata := n*digestLen*2 + blobIDLen
	return n*metadata + sliverSize
}

// EncodedStorageUnits returns the number of whole 1 MiB storage units billed
// for an encoded size. WAL cost is (units × epochs × price-per-unit-epoch).
func EncodedStorageUnits(encodedSize int64) int64 {
	if encodedSize <= 0 {
		return 0
	}
	return (encodedSize + storageUnitBytes - 1) / storageUnitBytes
}
