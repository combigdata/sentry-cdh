/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.SentryPolicyStorePlugin.SentryPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * UpdateForwarder caches incoming updates and passes them upon request to downstream cache.
 *
 * UpdateForwarder keeps the updates as a list with one full update as the head, followed
 * by subsequent partial updates. Updates should have strictly sequential sequence numbers;
 * sequence numbers with the gaps are possible, but not common, and should be logged.
 *
 * Keeping updates in this form enables two goals:
 *
 * a) If downstream cache fell far behind, due to recent restart or temporary connectivity
 *    issues, UpdateForwarder can send the entire updateLog, including full update, thus
 *    providing the entire current shapshot.
 *
 * b) If downstream cache is reasonably current, UpdateForwarder can send missing updates
 *    incrementally, as the tail of the updatesLog, starting from the sequence number
 *    specified in the request. The requester (downstream cache) is supposed to keep track
 *    of the sequence number of the last received update, in order to provide the correct
 *    starting sequence number for the next incremental update requests.
 *
 * Sending a full update is inevitable at initialization time of either Sentry or the 
 * downstream cache component, such as NameNode. During normal operations sending full
 * update should be avoided as much as possible. Full updates can be huge, their marshalling
 * and unmarshalling takes time and significant additional heap memory, and can lead to
 * high GC activity, system slowdown or even crash.
 *
 * UpdateForwarder is designed to minimize the need for full updates. It keeps the rolling
 * window of the latest updates, so that even if the downstream cache is behind (by not too
 * much), UpdateForwarder would still be able to provide missing partial updates without
 * having to include a full update.
 *
 * Keeping the updateLog's size limited is achieved by the "log compaction".
 * The updateLog is allowed to grow up to overflowUpdateLogSize, at which point it is
 * compacted down to maxUpdateLogSize (maxUpdateLogSize < overflowUpdateLogSize).
 *
 * During log compaction, the oldest updates are merged into a full image and deleted
 * from updateLog; then the updated full image is set as the head element of updateLog.
 * The sequence number of this full update is set to the sequence number of the last
 * compacted updateLog entry, so the entire list of updates remains strictly sequential.
 *
 * The reason why updateLog is not compacted on each update to keep its size constant,
 * is because regenerating a full image as the head of updateLog is a pretty expensive
 * operation, in terms of both time and memory. That's why compaction is performed only
 * when the log size reaches the "upper threshold value" overflowUpdateLogSize, and reduces
 * its size to the "lower threshold value" maxUpdateLogSize.
 *
 * You may find several places with error logs with "PANICK" in the log message. This is
 * for conditions that should in theory never happen. However, the code can be inadvertantly
 * broken, and those are critical constraints, which should always be verified, without
 * relying on assertions.
 */

public class UpdateForwarder<K extends Updateable.Update> implements Closeable {

  public static interface ExternalImageRetriever<K> {
    K retrieveFullImage(long currSeqNum) throws Exception;
  }

  /* Sequence number of the latest update passed to handleUpdateNotification() API. */
  private final AtomicLong lastSeenSeqNum = new AtomicLong(0);

  /* Sequence number of the latest update passed to handleUpdateNotification() API,
   * AFTER it's been appended to updateLog. This update happens on a separate thread,
   * initiated from handleUpdateNotification(), so lastCommittedSeqNum can be behind
   * lastSeenSeqNum for some short time.
   */
  protected final AtomicLong lastCommittedSeqNum = new AtomicLong(0);

  /* handleUpdateNotification() uses updateHandler to avoid blocking.
   * Processing incoming updates may involve regenerating a full update,
   * which may be time consuming.
   */
  private final ExecutorService updateHandler = Executors.newSingleThreadExecutor();

  /* updateLog stores update history as the full update followed by zero
   * or more partial updates, thus providing the capability to serve
   * either the entire updates history or in partial incrementsd.
   */
  private final LinkedList<K> updateLog = new LinkedList<K>();

  /* The size of updateLog after the log compaction.
   * updateLog is disabled when maxUpdateLogSize = 0;
   */
  private final int maxUpdateLogSize;
  /* the size of updateLog at which log compaction is performed */
  private final int overflowUpdateLogSize;

  /* The imageRetriever is the higher source of truth than "updateable".
   * The "updateable" integrates all updates that came to UpdateForwarder,
   * but when full image needs to be created, if imageRetriever is configured,
   * it is used in preference to "updateable". It can be database.
   */
  private final ExternalImageRetriever<K> imageRetriever;

  /* The object for storing a full updates image, to which partial updates
   * are merged at the log compaction time. This image matches the
   * head element in updatesLog.
   */
  private volatile Updateable<K> updateable;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  protected static final long INIT_SEQ_NUM = -2;
  protected static final int INIT_UPDATE_RETRY_DELAY = 5000;

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateForwarder.class);

  public UpdateForwarder(Configuration conf, Updateable<K> updateable,
      ExternalImageRetriever<K> imageRetriever, int maxUpdateLogSize) {
    this(conf, updateable, imageRetriever, maxUpdateLogSize, INIT_UPDATE_RETRY_DELAY);
  }

  protected UpdateForwarder(Configuration conf, Updateable<K> updateable, //NOPMD
      ExternalImageRetriever<K> imageRetriever, int maxUpdateLogSize,
      int initUpdateRetryDelay) {
    this.maxUpdateLogSize = maxUpdateLogSize;
    this.overflowUpdateLogSize = maxUpdateLogSize*2;
    this.imageRetriever = imageRetriever;
    if (imageRetriever != null) {
      spawnInitialUpdater(updateable, initUpdateRetryDelay);
    } else {
      this.updateable = updateable;
    }
    LOGGER.info("Started [{}] [maxUpdateLogSize={}] [updateable={}] [imageRetriever={}]",
      getClass().getSimpleName(),
      maxUpdateLogSize,
      (updateable != null) ? updateable.getClass().getSimpleName() : "null",
      (imageRetriever != null) ? imageRetriever.getClass().getSimpleName() : "null"
    );
  }

  public static <K extends Updateable.Update> UpdateForwarder<K> create(Configuration conf,
      Updateable<K> updateable, K update, ExternalImageRetriever<K> imageRetriever,
      int maxUpdateLogSize) throws SentryPluginException {
    return create(conf, updateable, update, imageRetriever, maxUpdateLogSize,
        INIT_UPDATE_RETRY_DELAY);
  }

  public static <K extends Updateable.Update> UpdateForwarder<K> create(Configuration conf,
      Updateable<K> updateable, K update, ExternalImageRetriever<K> imageRetriever,
      int maxUpdateLogSize, int initUpdateRetryDelay) throws SentryPluginException {
      return new UpdateForwarder<K>(conf, updateable, imageRetriever,
          maxUpdateLogSize, initUpdateRetryDelay);
  }

  private void spawnInitialUpdater(final Updateable<K> updateable,
      final int initUpdateRetryDelay) {
    K firstFullImage = null;
    try {
      firstFullImage = imageRetriever.retrieveFullImage(INIT_SEQ_NUM);
    } catch (Exception e) {
      LOGGER.warn("InitialUpdater encountered exception !! ", e);
      firstFullImage = null;
      Thread initUpdater = new Thread() {
        @Override
        public void run() {
          while (UpdateForwarder.this.updateable == null) {
            try {
              Thread.sleep(initUpdateRetryDelay);
            } catch (InterruptedException e) {
              LOGGER.warn("Thread interrupted !! ", e);
              break;
            }
            K fullImage = null;
            try {
              fullImage =
                  UpdateForwarder.this.imageRetriever.retrieveFullImage(INIT_SEQ_NUM);
              appendToUpdateLog(fullImage);
            } catch (Exception e) {
              LOGGER.warn("InitialUpdater encountered exception !! ", e);
            }
            if (fullImage != null) {
              UpdateForwarder.this.updateable = updateable.updateFull(fullImage);
            }
          }
        }
      };
      initUpdater.start();
    }

    if (firstFullImage != null) {
      try {
        appendToUpdateLog(firstFullImage);
      } catch (Exception e) {
        LOGGER.warn("failed to update append log: ", e);
      }
      this.updateable = updateable.updateFull(firstFullImage);
    }
  }
  /**
   * Handle notifications from HMS plug-in or upstream Cache
   * @param update
   */
  public void handleUpdateNotification(final K update) throws SentryPluginException {
    // Correct the seqNums on the first update
    if (lastCommittedSeqNum.get() == INIT_SEQ_NUM) {
      K firstUpdate = updateLog.peek();
      long firstSeqNum = update.getSeqNum() - 1;
      if (firstUpdate != null) {
        firstUpdate.setSeqNum(firstSeqNum);
      }
      lastCommittedSeqNum.set(firstSeqNum);
      lastSeenSeqNum.set(firstSeqNum);
      LOGGER.info("[{}]: First Update Notification [{}] [{}]",
        update.getClass().getSimpleName(), update.getSeqNum(), lastSeenSeqNum.get());
    }

    final boolean editMissed =
        lastSeenSeqNum.incrementAndGet() != update.getSeqNum();
    if (editMissed) {
      LOGGER.warn("[{}]: Update Notification: out-of-sync [{}] [{}]",
        update.getClass().getSimpleName(), lastSeenSeqNum.get(), update.getSeqNum());
      lastSeenSeqNum.set(update.getSeqNum());
    }

    // This task will detect special conditions that require resetting the
    // updateable (and update) to full image. Then it delegates the rest of
    // work to appendToUpdateLog().
    // It does NOT merge partial updates, only full ones!!!
    Runnable task = new Runnable() {
      @Override
      public void run() {
        K toUpdate = update;
        if (update.hasFullImage()) {
          LOGGER.warn("[{}]: Full Image Arrived: [{}]", update.getClass().getSimpleName(), update.getSeqNum());
          updateable = updateable.updateFull(update); // merge full update
        } else {
          if (editMissed) {
            // Retrieve full update from External Source
            if (imageRetriever != null) {
              LOGGER.warn("[{}]: FULL Update Required due to out-of-sync: [expectedSeqNum {}] [seqNum {}]",
                update.getClass().getSimpleName(), lastSeenSeqNum.get(),  update.getSeqNum());
              try {
                toUpdate = imageRetriever.retrieveFullImage(update.getSeqNum());
              } catch (Exception e) {
                LOGGER.warn("failed to retrieve full image: ", e);
              }
              updateable = updateable.updateFull(toUpdate); // merge full update
            } else {
              LOGGER.warn("[{}]: Update Notification: out-of-sync [{}]: Cannot retrieve Full Image, no retriever",
                update.getClass().getSimpleName(), update.getSeqNum());
            }
          }
        }

        try {
          appendToUpdateLog(toUpdate);
        } catch (Exception e) {
          LOGGER.warn("failed to append to update log", e);
        }
      }
    };
    updateHandler.execute(task);
  }

  /**
   * This method works as follows:
   * a) If the update is full image, the caller has already merged it into "updateable".
   *    All left is to reset the updateLog to this single full update ("beginning of history").
   * b) if the update is partial, append it to updateLog, then perform compaction is needed.
   */
  private void appendToUpdateLog(final K update) throws Exception {
    synchronized (updateLog) {
      boolean logCompacted = false;
      if (maxUpdateLogSize > 0) {
        // most common case of partial update
        if (!update.hasFullImage()) {
          updateLog.add(update); // first, append partial update to the log
          // compact the log if needed
          if (updateLog.size() >= overflowUpdateLogSize) {
            LOGGER.info("{} - UpdateLog Reached Overflow Size {}, Compacting ...", getUpdateInfo(update), overflowUpdateLogSize);

            // remove first (full image) log entry - we won't need it because full image will be updated
            final K headUpdate = updateLog.removeFirst();
            if (headUpdate.hasFullImage()) { // the first entry must always be full image
              // The old updates are compacted just enough for the log to be down to its normal size.
              // All removed partial updates are merged into the "updateable".
              // The latest partial updates are preserved.
              long lastSeqNum = headUpdate.getSeqNum();
              final List<K> compactedUpdates = new ArrayList<>(overflowUpdateLogSize - maxUpdateLogSize +1);
              while (updateLog.size() >= maxUpdateLogSize) {
                K upd = updateLog.removeFirst();
                if (upd.hasFullImage()) { // can never happen
                  LOGGER.error("{} - PANICK: COMPACTED ELEMENT {} IS FULL IMAGE !!!", getUpdateInfo(update), upd.getSeqNum());
                }
                if (upd.getSeqNum() != (lastSeqNum+1)) { // can never happen
                  LOGGER.error("{} - PANICK: COMPACTED ELEMENT OUT-OF-SEQUENCE [{} , {}]", getUpdateInfo(update), lastSeqNum, upd.getSeqNum());
                }
                lastSeqNum = upd.getSeqNum();
                compactedUpdates.add(upd);
              }
              // merge removed updates into updateable
              updateable.updatePartial(compactedUpdates, lock);
              // finally, create full update with the sequence number of the last removed partial update
              // and make it the first element in the compacted updates log
              K newHeadUpdate = createFullImageUpdate(compactedUpdates.get(compactedUpdates.size() -1).getSeqNum());
              updateLog.addFirst(newHeadUpdate);

              LOGGER.info("{} - UpdateLog Overflow, Compacting Done", getUpdateInfo(update));
              logCompacted = true;
            } else { // can never happen
              LOGGER.error("{} - PANICK: HEAD [seqNum={}] NOT FULL IMAGE !!!", getUpdateInfo(update), headUpdate.getSeqNum());
            }
          }
        } else { // update is a full image - reinitialize the log
          updateLog.clear();
          updateLog.add(update);
          LOGGER.info("{} - DONE", getUpdateInfo(update));
        }
      }
                
      lastCommittedSeqNum.set(update.getSeqNum());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("#### {} - logCompacted={}", getUpdateInfo(update), logCompacted);
      }
    }
  }

  // helper to print out all essential info about the update notification in progress
  private String getUpdateInfo(K update) {
    return String.format("%s[seqNum=%d, %s]: %s",
      update.getClass().getSimpleName(),
      update.getSeqNum(),
      (update.hasFullImage() ? "FULL" : "Partial"),
      getStateInfo());
  }

  // helper to print out the current state of update retrieving in progress
  private String getUpdateableInfo(long seqNum) {
    return String.format("getAllUpdatesFrom(%d): %s, %s",
      seqNum,
      (updateable != null) ? updateable.getClass().getSimpleName() : "null updateable",
      getStateInfo());
  }

  // helper to print out the current state of UpdateForwarder
  private String getStateInfo() {
    return String.format("lastCommit=%d, lastSeen=%d, update_log[%d](%d , %d)",
      lastCommittedSeqNum.get(),
      lastSeenSeqNum.get(),
      updateLog.size(),
      updateLog.isEmpty() ? -1 : updateLog.getFirst().getSeqNum(),
      updateLog.isEmpty() ? -1 : updateLog.getLast().getSeqNum());
  }

  /**
   * Return all updates from requested seqNum (inclusive)
   * @param seqNum asking for updates with sequence number >= seqNum
   * @return list of updates
   */
  public List<K> getAllUpdatesFrom(final long seqNum) throws Exception {
    final List<K> retVal = new LinkedList<K>();
    synchronized (updateLog) {
      long currSeqNum = lastCommittedSeqNum.get();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("{} - begin", getUpdateableInfo(seqNum));
      }

      // no updatelog configured..
      // Why not to disable passing maxUpdateLogSize < 0 to ctor?
      // not sure, but this is the old code
      if (maxUpdateLogSize == 0) {
        return retVal;
      }

      K head = updateLog.peek();
      // updateLog can be empty at the initialization, when UpdateForwarder
      // has been constructed, but initialization from spawnInitialUpdater()
      // has not yet completed. Likely will be complete by the time of the
      // next poll; return nothing at this time.
      if (head == null) {
        LOGGER.warn("{}: initializing ...", getUpdateableInfo(seqNum));
        return retVal;
      }

      // This Sentry instance has probably restarted since downstream
      // recieved last update. Return the entire history.
      if (seqNum > currSeqNum + 1) {
        retVal.addAll(updateLog);
        LOGGER.warn("{}: potential restart ???", getUpdateableInfo(seqNum));
        return retVal;
      }

      // The caller is behind the oldest update in updateLog, which is not normal.
      // Have to send the entire log, which, infortunately, includes the head entry
      // with full image, which we are trying to avoid. Can happen for two reasons:
      // 
      // a) high transactions volume; updates happen too frequently, and configured
      //    updateLog size is too small, not going enough back into the past.
      // b) downstream cache component skipped a few update polls due to system slowdown
      //    or connectivity problem.
      // 
      // In any event, event should be logged and we must insure that the head element
      // in updateLog is a full imagae.
      if (head.getSeqNum() > seqNum) {
        if (head.hasFullImage()) { // The head, if exists, must always be a full image
          // Send full image along with partial updates.
          for (K u : updateLog) {
            retVal.add(u);
          }
          LOGGER.warn("{}: diverged caller, returning full image plus partial updates from [{}]",
            getUpdateableInfo(seqNum), head.getSeqNum());
          // The head log entry is not full image. Fix it.
          // Create a full image; clear updateLog; add fullImage to head of Log.
          // NOTE : This should ideally never happen
        } else {
          K fullImage = createFullImageUpdate(currSeqNum);
          if (fullImage != null) {
            LOGGER.info("{}: compacting to full update {}[{}]", getUpdateableInfo(seqNum),
              fullImage.getClass().getSimpleName(), fullImage.getSeqNum());
          } else { // should never happen
            LOGGER.error("{}: PANICK: compacting to NULL full update ???", getUpdateableInfo(seqNum));
          }
          updateLog.clear();
          updateLog.add(fullImage);
          retVal.add(fullImage);
        }
      // Most common case - asked sequence number exists in the current update log.
      } else {
        // return updates with sequence numbers >= the requested seqNum
        for (K elem : updateLog) {
          if (elem.getSeqNum() >= seqNum) {
            retVal.add(elem);
          }
        }
      }
    }
    return retVal;
  }

  // required by SentryPlugin, which is in the same package
  long getLastSeen() {
    return lastSeenSeqNum.get();
  }

  // required for unit testing only
  public boolean areAllUpdatesCommited() {
    return lastCommittedSeqNum.get() == lastSeenSeqNum.get();
  }

  // required for unit testing only
  public long getLastUpdatedSeqNum() {
    return lastCommittedSeqNum.get();
  }

  private K createFullImageUpdate(long currSeqNum) throws Exception {
    return (updateable != null) ? updateable.createFullImageUpdate(currSeqNum) : null;
  }

  @Override
  public void close() throws IOException {
    updateHandler.shutdownNow();
  }
}
