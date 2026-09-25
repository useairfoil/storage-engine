// Model of Storage Engine ingestion from wip/0001-reconciliation-loop.md.
//
// The model follows OSWALD's append protocol. Writers discover the first
// missing WAL sequence number, create that chunk with If-None-Match, and then
// validate the table metadata version before acknowledging the batch. A
// metadata change is harmless unless pruning has reached the writer's last
// validated safe sequence number.
//
// Publishing and garbage collection are represented only by the environment
// actions needed to exercise ingestion races. `publishNext` atomically models
// an already-created table snapshot becoming authoritative; a full publishing
// model can refine that action later.
//
// Suggested reading order:
// 1. Read the types, finite domains, and state variables to learn what is
//    durable, volatile, and ghost state.
// 2. Read `init`, then follow the successful writer path: `submitBatch`,
//    `startAppend`, `discoverOccupiedSeqNum`, `finishHeadDiscovery`,
//    `putWalChunk`, and `validateAppend`.
// 3. Read `observeAppendConflict`, `losePutResponse`, and `crashWriter` for
//    retry and failure behavior.
// 4. Read `publishNext`, `advanceGcSeqNum`, and `deleteGcChunk` as the
//    concurrent environment that can invalidate a writer.
// 5. Finish with the properties, `step`, and the scenario tests at the bottom;
//    the tests provide concrete traces for the important interleavings.
module ingestion {
  import object_storage as s3 from "./object_storage"

  type WriterId = str

  // A Batch abstracts the exact serialized rows in one WAL append. Equal
  // values mean equal primary keys, versions, and row values; this is not a
  // requirement for Storage Engine to persist a separate batch identifier.
  type Batch = str
  type SeqNum = int
  type ObjectKey = MetadataKey | WalKey(SeqNum)

  // Quint has no storage-level Option encoding requirement, so -1 represents
  // the RFC's initial None for both sequence-number frontiers.
  pure val NO_SEQ_NUM: SeqNum = -1
  pure val MAX_SEQ_NUM: SeqNum = 10
  pure val SEQ_NUMS: Set[SeqNum] = 0.to(MAX_SEQ_NUM)
  pure val WRITER_IDS: Set[WriterId] = Set("writer-1", "writer-2")
  pure val BATCHES: Set[Batch] = Set("batch-1", "batch-2")

  // `publishedSeqNum` is a contiguous prefix incorporated into table data.
  // `gcSeqNum` is a logical watermark: chunks at or below it may be gone.
  type TableMetadata = {
    publishedSeqNum: SeqNum,
    gcSeqNum: SeqNum,
  }

  // The generic object-storage model needs one data type for every key. This
  // envelope distinguishes mutable table metadata from immutable WAL chunks.
  // Unused fields contain their initial/empty values.
  type ObjectData = {
    kind: str,
    tableMetadata: TableMetadata,
    batch: Batch,
  }

  type Attempt = {
    batch: Batch,
    observedMetadataETag: int,
    safeSeqNum: SeqNum,
    nextSeqNum: SeqNum,
  }

  // Discovering scans occupied positions after the published prefix. Ready
  // has validated that nextSeqNum was absent. PutSucceeded represents the
  // deliberate pause between a successful PUT and metadata validation.
  type WriterState =
    | Idle
    | Discovering(Attempt)
    | Ready(Attempt)
    | PutSucceeded(Attempt)

  pure val METADATA_KEY: ObjectKey = MetadataKey
  pure val initialMetadata: TableMetadata = {
    publishedSeqNum: NO_SEQ_NUM,
    gcSeqNum: NO_SEQ_NUM,
  }

  pure def tableStateData(tableMetadata: TableMetadata): ObjectData = {
    kind: "table-metadata",
    tableMetadata: tableMetadata,
    batch: "",
  }

  pure def walChunkData(batch: Batch): ObjectData = {
    kind: "wal-chunk",
    tableMetadata: initialMetadata,
    batch: batch,
  }

  pure def walKey(seqNum: SeqNum): ObjectKey = WalKey(seqNum)

  pure def asMetadata(data: ObjectData): TableMetadata = data.tableMetadata

  pure def asBatch(data: ObjectData): Batch = data.batch

  def objectAt(key: ObjectKey): s3::StoredObject[ObjectData] =
    s3::getObject(key)

  val metadataObject: s3::StoredObject[ObjectData] =
    objectAt(METADATA_KEY)
  val metadata: TableMetadata = asMetadata(metadataObject.data)

  def hasWalChunk(seqNum: SeqNum): bool =
    if (s3::objects.keys().contains(walKey(seqNum)))
      objectAt(walKey(seqNum)).data.kind == "wal-chunk"
    else false

  def walBatch(seqNum: SeqNum): Batch =
    if (hasWalChunk(seqNum))
      asBatch(objectAt(walKey(seqNum)).data)
    else ""

  pure def discoveringFrom(batch: Batch, current: s3::StoredObject[ObjectData]): WriterState = {
    val currentMetadata = asMetadata(current.data)
    Discovering({
      batch: batch,
      observedMetadataETag: current.etag,
      safeSeqNum: currentMetadata.publishedSeqNum,
      nextSeqNum: currentMetadata.publishedSeqNum + 1,
    })
  }

  action keepStorage = all {
    s3::objects' = s3::objects,
    s3::nextETag' = s3::nextETag,
  }

  var writers: WriterId -> WriterState
  var submittedBatches: Set[Batch]
  var ackedBatches: Set[Batch]

  // Ghost state representing the logical rows in the current table snapshot.
  // Set union captures the RFC's requirement that publishing duplicate batches
  // has the same logical result as publishing one copy.
  var publishedBatches: Set[Batch]

  // A disabled branch still frames every state variable so all branches of an
  // action have the same effect signature.
  action disabled = all {
    false,
    keepStorage,
    writers' = writers,
    submittedBatches' = submittedBatches,
    ackedBatches' = ackedBatches,
    publishedBatches' = publishedBatches,
  }

  action init = all {
    s3::objects' = Map(
      METADATA_KEY -> {
        data: tableStateData(initialMetadata),
        etag: 0,
      }
    ),
    s3::nextETag' = 1,
    writers' = WRITER_IDS.mapBy(_ => Idle),
    submittedBatches' = Set(),
    ackedBatches' = Set(),
    publishedBatches' = Set(),
  }

  // The client submits one logical batch. Until it is acknowledged, any idle
  // writer may start or retry an append for it.
  action submitBatch(batch: Batch): bool = all {
    BATCHES.contains(batch),
    not(submittedBatches.contains(batch)),
    keepStorage,
    writers' = writers,
    submittedBatches' = submittedBatches.union(Set(batch)),
    ackedBatches' = ackedBatches,
    publishedBatches' = publishedBatches,
  }

  action startAppend(writer: WriterId, batch: Batch): bool = all {
    WRITER_IDS.contains(writer),
    submittedBatches.contains(batch),
    not(ackedBatches.contains(batch)),
    writers.get(writer) == Idle,
    metadata.publishedSeqNum < MAX_SEQ_NUM,
    keepStorage,
    writers' = writers.put(writer, discoveringFrom(batch, metadataObject)),
    submittedBatches' = submittedBatches,
    ackedBatches' = ackedBatches,
    publishedBatches' = publishedBatches,
  }

  // Catch up without interpreting chunk contents. Publishing is the WAL
  // consumer; ingestion only has to find the first unoccupied sequence number.
  action discoverOccupiedSeqNum(writer: WriterId): bool =
    match writers.get(writer) {
      | Discovering(attempt) => all {
          attempt.nextSeqNum < MAX_SEQ_NUM,
          hasWalChunk(attempt.nextSeqNum),
          keepStorage,
          writers' = writers.put(
            writer,
            Discovering({
              batch: attempt.batch,
              observedMetadataETag: attempt.observedMetadataETag,
              safeSeqNum: attempt.safeSeqNum,
              nextSeqNum: attempt.nextSeqNum + 1,
            })
          ),
          submittedBatches' = submittedBatches,
          ackedBatches' = ackedBatches,
          publishedBatches' = publishedBatches,
        }
      | _ => disabled
    }

  // Validate head discovery like OSWALD tailing. If pruning invalidated the
  // starting snapshot, restart from the fresh metadata. Otherwise the first
  // missing position becomes the writer's new safe point.
  action finishHeadDiscovery(writer: WriterId): bool =
    match writers.get(writer) {
      | Discovering(attempt) => {
          val metadataChanged =
            metadataObject.etag != attempt.observedMetadataETag
          val invalidated =
            metadataChanged and attempt.safeSeqNum <= metadata.gcSeqNum
          all {
            attempt.nextSeqNum <= MAX_SEQ_NUM,
            not(hasWalChunk(attempt.nextSeqNum)),
            keepStorage,
            writers' = writers.put(
              writer,
              if (invalidated) discoveringFrom(attempt.batch, metadataObject)
              else Ready({
                batch: attempt.batch,
                observedMetadataETag: metadataObject.etag,
                safeSeqNum: attempt.nextSeqNum,
                nextSeqNum: attempt.nextSeqNum,
              })
            ),
            submittedBatches' = submittedBatches,
            ackedBatches' = ackedBatches,
            publishedBatches' = publishedBatches,
          }
        }
      | _ => disabled
    }

  // A writer can lose the race after discovering an absent position. It must
  // return to head discovery rather than overwrite the winning chunk.
  action observeAppendConflict(writer: WriterId): bool =
    match writers.get(writer) {
      | Ready(attempt) => all {
          hasWalChunk(attempt.nextSeqNum),
          keepStorage,
          writers' = writers.put(writer, Discovering(attempt)),
          submittedBatches' = submittedBatches,
          ackedBatches' = ackedBatches,
          publishedBatches' = publishedBatches,
        }
      | _ => disabled
    }

  action putWalChunk(writer: WriterId): bool =
    match writers.get(writer) {
      | Ready(attempt) => all {
          attempt.nextSeqNum <= MAX_SEQ_NUM,
          s3::putObject(
            walKey(attempt.nextSeqNum),
            walChunkData(attempt.batch),
            s3::Create
          ),
          writers' = writers.put(writer, PutSucceeded(attempt)),
          submittedBatches' = submittedBatches,
          ackedBatches' = ackedBatches,
          publishedBatches' = publishedBatches,
        }
      | _ => disabled
    }

  // The successful PUT is not enough to acknowledge. A changed metadata ETag
  // requires checking whether pruning reached the writer's safe point. If it
  // did, the writer cannot distinguish an included append from a stale write
  // into a deleted name, so at-least-once ingestion retries from fresh state.
  action validateAppend(writer: WriterId): bool =
    match writers.get(writer) {
      | PutSucceeded(attempt) => {
          val metadataChanged =
            metadataObject.etag != attempt.observedMetadataETag
          val invalidated =
            metadataChanged and attempt.safeSeqNum <= metadata.gcSeqNum
          all {
            keepStorage,
            writers' = writers.put(
              writer,
              if (invalidated)
                discoveringFrom(attempt.batch, metadataObject)
              else Idle
            ),
            submittedBatches' = submittedBatches,
            ackedBatches' =
              if (invalidated) ackedBatches
              else ackedBatches.union(Set(attempt.batch)),
            publishedBatches' = publishedBatches,
          }
        }
      | _ => disabled
    }

  // Model a successful object-store write whose response is lost. The client
  // retains an unacknowledged logical request and may append the same batch at
  // another sequence number, producing the RFC's at-least-once behavior.
  action losePutResponse(writer: WriterId): bool =
    match writers.get(writer) {
      | PutSucceeded(_) => all {
          keepStorage,
          writers' = writers.put(writer, Idle),
          submittedBatches' = submittedBatches,
          ackedBatches' = ackedBatches,
          publishedBatches' = publishedBatches,
        }
      | _ => disabled
    }

  // All writer state is volatile. A crash before acknowledgement leaves the
  // submitted batch eligible for retry by any writer.
  action crashWriter(writer: WriterId): bool =
    match writers.get(writer) {
      | Idle => disabled
      | _ => all {
          keepStorage,
          writers' = writers.put(writer, Idle),
          submittedBatches' = submittedBatches,
          ackedBatches' = ackedBatches,
          publishedBatches' = publishedBatches,
        }
    }

  // Abstract one successful publishing commit. Advancing one position at a
  // time encodes the contract that publishedSeqNum is a contiguous prefix.
  action publishNext: bool = {
    val nextSeqNum = metadata.publishedSeqNum + 1
    val batch = walBatch(nextSeqNum)
    val nextMetadata = {
      publishedSeqNum: nextSeqNum,
      gcSeqNum: metadata.gcSeqNum,
    }
    all {
      nextSeqNum <= MAX_SEQ_NUM,
      hasWalChunk(nextSeqNum),
      s3::putObject(
        METADATA_KEY,
        tableStateData(nextMetadata),
        s3::Update(metadataObject.etag)
      ),
      writers' = writers,
      submittedBatches' = submittedBatches,
      ackedBatches' = ackedBatches,
      publishedBatches' = publishedBatches.union(Set(batch)),
    }
  }

  // OSWALD publishes the logical GC watermark before deleting any chunks.
  // This action may jump over several positions to model a long writer pause.
  action advanceGcSeqNum: bool = {
    val nextMetadata = {
      publishedSeqNum: metadata.publishedSeqNum,
      gcSeqNum: metadata.publishedSeqNum,
    }
    all {
      metadata.gcSeqNum < metadata.publishedSeqNum,
      s3::putObject(
        METADATA_KEY,
        tableStateData(nextMetadata),
        s3::Update(metadataObject.etag)
      ),
      writers' = writers,
      submittedBatches' = submittedBatches,
      ackedBatches' = ackedBatches,
      publishedBatches' = publishedBatches,
    }
  }

  action deleteGcChunk(seqNum: SeqNum): bool = all {
    SEQ_NUMS.contains(seqNum),
    seqNum <= metadata.gcSeqNum,
    hasWalChunk(seqNum),
    s3::deleteObject(walKey(seqNum)),
    writers' = writers,
    submittedBatches' = submittedBatches,
    ackedBatches' = ackedBatches,
    publishedBatches' = publishedBatches,
  }

  // --- Properties ---------------------------------------------------------

  val metadataHasExpectedType: bool =
    s3::objects.keys().contains(METADATA_KEY) and
      metadataObject.data.kind == "table-metadata"

  val metadataFrontiersAreValid: bool = and {
    NO_SEQ_NUM <= metadata.gcSeqNum,
    metadata.gcSeqNum <= metadata.publishedSeqNum,
    metadata.publishedSeqNum <= MAX_SEQ_NUM,
  }

  // Unpublished chunks form a contiguous suffix immediately after the
  // published prefix. Chunks in the published prefix may have been pruned.
  val unprunedWalHasNoGaps: bool =
    SEQ_NUMS.forall(seqNum =>
      if (seqNum > metadata.publishedSeqNum and hasWalChunk(seqNum))
        SEQ_NUMS
          .filter(previous =>
            metadata.publishedSeqNum < previous and previous < seqNum
          )
          .forall(hasWalChunk)
      else true
    )

  val retainedWalBatches: Set[Batch] =
    SEQ_NUMS
      .filter(seqNum =>
        metadata.gcSeqNum < seqNum and hasWalChunk(seqNum)
      )
      .fold(
        Set(),
        (batches, seqNum) => batches.union(Set(walBatch(seqNum)))
      )

  // The central ingestion safety property: pruning cannot remove the only
  // durable representation of an acknowledged batch.
  val ackedBatchesAreDurable: bool =
    ackedBatches.subseteq(
      retainedWalBatches.union(publishedBatches)
    )

  val onlySubmittedBatchesAreAcked: bool =
    ackedBatches.subseteq(submittedBatches)

  val invariants: bool = and {
    metadataHasExpectedType,
    metadataFrontiersAreValid,
    unprunedWalHasNoGaps,
    ackedBatchesAreDurable,
    onlySubmittedBatchesAreAcked,
  }

  action step = {
    nondet writer = WRITER_IDS.oneOf()
    nondet batch = BATCHES.oneOf()
    nondet seqNum = SEQ_NUMS.oneOf()
    any {
      submitBatch(batch),
      startAppend(writer, batch),
      discoverOccupiedSeqNum(writer),
      finishHeadDiscovery(writer),
      observeAppendConflict(writer),
      putWalChunk(writer),
      validateAppend(writer),
      losePutResponse(writer),
      crashWriter(writer),
      publishNext,
      advanceGcSeqNum,
      deleteGcChunk(seqNum),
    }
  }
}

module ingestion_tests {
  import ingestion.*

  run appendAndAckTest =
    init
      .then(submitBatch("batch-1"))
      .then(startAppend("writer-1", "batch-1"))
      .then(finishHeadDiscovery("writer-1"))
      .then(putWalChunk("writer-1"))
      .then(validateAppend("writer-1"))
      .expect(
        walBatch(0) == "batch-1" and
        ackedBatches == Set("batch-1") and
        ackedBatchesAreDurable
      )

  run concurrentWritersUseDifferentSeqNumsTest =
    init
      .then(submitBatch("batch-1"))
      .then(submitBatch("batch-2"))
      .then(startAppend("writer-1", "batch-1"))
      .then(startAppend("writer-2", "batch-2"))
      .then(finishHeadDiscovery("writer-1"))
      .then(finishHeadDiscovery("writer-2"))
      .then(putWalChunk("writer-1"))
      .then(observeAppendConflict("writer-2"))
      .then(discoverOccupiedSeqNum("writer-2"))
      .then(finishHeadDiscovery("writer-2"))
      .then(putWalChunk("writer-2"))
      .then(validateAppend("writer-1"))
      .then(validateAppend("writer-2"))
      .expect(
        walBatch(0) == "batch-1" and
        walBatch(1) == "batch-2" and
        ackedBatches == Set("batch-1", "batch-2") and
        unprunedWalHasNoGaps
      )

  run lostResponseProducesAtLeastOnceDeliveryTest =
    init
      .then(submitBatch("batch-1"))
      .then(startAppend("writer-1", "batch-1"))
      .then(finishHeadDiscovery("writer-1"))
      .then(putWalChunk("writer-1"))
      .then(losePutResponse("writer-1"))
      .then(startAppend("writer-1", "batch-1"))
      .then(discoverOccupiedSeqNum("writer-1"))
      .then(finishHeadDiscovery("writer-1"))
      .then(putWalChunk("writer-1"))
      .then(validateAppend("writer-1"))
      .expect(
        walBatch(0) == "batch-1" and
        walBatch(1) == "batch-1" and
        ackedBatches == Set("batch-1") and
        ackedBatchesAreDurable
      )

  // A publication-only metadata change does not invalidate a completed PUT.
  run publishBetweenPutAndValidationTest =
    init
      .then(submitBatch("batch-1"))
      .then(startAppend("writer-1", "batch-1"))
      .then(finishHeadDiscovery("writer-1"))
      .then(putWalChunk("writer-1"))
      .then(publishNext)
      .then(validateAppend("writer-1"))
      .expect(
        metadata.publishedSeqNum == 0 and
        publishedBatches == Set("batch-1") and
        ackedBatches == Set("batch-1") and
        ackedBatchesAreDurable
      )

  // If publishing and pruning pass a paused writer, it cannot tell whether its
  // PUT preceded the snapshot or recreated a deleted name. It recovers and may
  // append the same batch again before acknowledging it.
  run pruneBetweenPutAndValidationForcesRetryTest =
    init
      .then(submitBatch("batch-1"))
      .then(startAppend("writer-1", "batch-1"))
      .then(finishHeadDiscovery("writer-1"))
      .then(putWalChunk("writer-1"))
      .then(publishNext)
      .then(advanceGcSeqNum)
      .then(deleteGcChunk(0))
      .then(validateAppend("writer-1"))
      .expect(
        ackedBatches == Set() and
        publishedBatches == Set("batch-1")
      )
      .then(finishHeadDiscovery("writer-1"))
      .then(putWalChunk("writer-1"))
      .then(validateAppend("writer-1"))
      .expect(
        walBatch(1) == "batch-1" and
        ackedBatches == Set("batch-1") and
        ackedBatchesAreDurable
      )

  // The dangerous ordering from OSWALD: GC deletes a sequence before a stale
  // writer creates that old name. Post-PUT validation must reject the append.
  run staleWriterCannotAckRecreatedChunkTest =
    init
      .then(submitBatch("batch-1"))
      .then(submitBatch("batch-2"))
      .then(startAppend("writer-1", "batch-1"))
      .then(startAppend("writer-2", "batch-2"))
      .then(finishHeadDiscovery("writer-1"))
      .then(finishHeadDiscovery("writer-2"))
      .then(putWalChunk("writer-2"))
      .then(validateAppend("writer-2"))
      .then(publishNext)
      .then(advanceGcSeqNum)
      .then(deleteGcChunk(0))
      .then(putWalChunk("writer-1"))
      .then(validateAppend("writer-1"))
      .expect(
        ackedBatches == Set("batch-2") and
        publishedBatches == Set("batch-2") and
        ackedBatchesAreDurable
      )
}
