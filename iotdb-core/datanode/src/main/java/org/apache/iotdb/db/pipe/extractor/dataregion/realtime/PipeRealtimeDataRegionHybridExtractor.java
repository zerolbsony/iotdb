/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.dataregion.IoTDBDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner.PipeTsFileEpochProgressIndexKeeper;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeRemainingEventAndTimeOperator;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionExtractorMetrics;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

public class PipeRealtimeDataRegionHybridExtractor extends PipeRealtimeDataRegionExtractor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeRealtimeDataRegionHybridExtractor.class);

  @Override
  protected void doExtract(final PipeRealtimeEvent event) {
    final Event eventToExtract = event.getEvent();

    if (eventToExtract instanceof TabletInsertionEvent) {
      extractTabletInsertion(event);
    } else if (eventToExtract instanceof TsFileInsertionEvent) {
      extractTsFileInsertion(event);
    } else if (eventToExtract instanceof PipeHeartbeatEvent) {
      extractHeartbeat(event);
    } else if (eventToExtract instanceof PipeDeleteDataNodeEvent) {
      extractDirectly(event);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Unsupported event type %s for hybrid realtime extractor %s",
              eventToExtract.getClass(), this));
    }
  }

  @Override
  public boolean isNeedListenToTsFile() {
    return shouldExtractInsertion;
  }

  @Override
  public boolean isNeedListenToInsertNode() {
    return shouldExtractInsertion;
  }

  private void extractTabletInsertion(final PipeRealtimeEvent event) {
    TsFileEpoch.State state;

    if (canNotUseTabletAnyMore(event)) {
      event.getTsFileEpoch().migrateState(this, curState -> TsFileEpoch.State.USING_TSFILE);
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .registerProgressIndex(dataRegionId, pipeName, event.getTsFileEpoch().getResource());
    } else {
      event
          .getTsFileEpoch()
          .migrateState(
              this,
              curState -> {
                switch (curState) {
                  case USING_BOTH:
                  case USING_TSFILE:
                    return TsFileEpoch.State.USING_BOTH;
                  case EMPTY:
                  case USING_TABLET:
                  default:
                    return TsFileEpoch.State.USING_TABLET;
                }
              });
    }

    state = event.getTsFileEpoch().getState(this);
    switch (state) {
      case USING_TSFILE:
        // Ignore the tablet event.
        event.decreaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
        break;
      case EMPTY:
      case USING_TABLET:
      case USING_BOTH:
        // USING_BOTH indicates that there are discarded events previously.
        // In this case, we need to delay the progress report to tsFile event, to avoid losing data.
        if (state == TsFileEpoch.State.USING_BOTH) {
          event.skipReportOnCommit();
        }
        if (!pendingQueue.waitedOffer(event)) {
          // This would not happen, but just in case.
          // pendingQueue is unbounded, so it should never reach capacity.
          final String errorMessage =
              String.format(
                  "extractTabletInsertion: pending queue of PipeRealtimeDataRegionHybridExtractor %s "
                      + "has reached capacity, discard tablet event %s, current state %s",
                  this, event, event.getTsFileEpoch().getState(this));
          LOGGER.error(errorMessage);
          PipeDataNodeAgent.runtime()
              .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

          // Ignore the tablet event.
          event.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported state %s for hybrid realtime extractor %s",
                state, PipeRealtimeDataRegionHybridExtractor.class.getName()));
    }
  }

  private void extractTsFileInsertion(final PipeRealtimeEvent event) {
    // Notice that, if the tsFile is partially extracted because the pipe is not opened before, the
    // former data won't be extracted
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              switch (state) {
                case EMPTY:
                  return ((PipeTsFileInsertionEvent) event.getEvent()).isLoaded()
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_TABLET;
                case USING_TABLET:
                  return TsFileEpoch.State.USING_TABLET;
                case USING_TSFILE:
                  return TsFileEpoch.State.USING_TSFILE;
                case USING_BOTH:
                default:
                  return canNotUseTabletAnyMore(event)
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_BOTH;
              }
            });

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    switch (state) {
      case USING_TABLET:
        // Though the data in tsfile event has been extracted in tablet mode, we still need to
        // extract the tsfile event to help to determine isTsFileEventCountInQueueExceededLimit().
        // The extracted tsfile event will be discarded in supplyTsFileInsertion.
      case EMPTY:
      case USING_TSFILE:
      case USING_BOTH:
        if (!pendingQueue.waitedOffer(event)) {
          // This would not happen, but just in case.
          // pendingQueue is unbounded, so it should never reach capacity.
          final String errorMessage =
              String.format(
                  "extractTsFileInsertion: pending queue of PipeRealtimeDataRegionHybridExtractor %s "
                      + "has reached capacity, discard TsFile event %s, current state %s",
                  this, event, event.getTsFileEpoch().getState(this));
          LOGGER.error(errorMessage);
          PipeDataNodeAgent.runtime()
              .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));

          // Ignore the tsfile event.
          event.decreaseReferenceCount(
              PipeRealtimeDataRegionHybridExtractor.class.getName(), false);
        }
        break;
      default:
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported state %s for hybrid realtime extractor %s",
                state, PipeRealtimeDataRegionHybridExtractor.class.getName()));
    }
  }

  private boolean canNotUseTabletAnyMore(final PipeRealtimeEvent event) {
    // In the following 4 cases, we should not extract this tablet event. all the data
    // represented by the tablet event should be carried by the following tsfile event:
    //  the write operation will be throttled, so we should not extract any more tablet events.
    //  1. The shallow memory usage of the insert node has reached the dangerous threshold.
    //  2. Deprecated logics (unused by default)
    return mayInsertNodeMemoryReachDangerousThreshold(event)
        || canNotUseTabletAnymoreDeprecated(event);
  }

  private boolean mayInsertNodeMemoryReachDangerousThreshold(final PipeRealtimeEvent event) {
    final long floatingMemoryUsageInByte =
        PipeDataNodeAgent.task().getFloatingMemoryUsageInByte(pipeName);
    final long pipeCount = PipeDataNodeAgent.task().getPipeCount();
    final long totalFloatingMemorySizeInBytes =
        PipeDataNodeResourceManager.memory().getTotalFloatingMemorySizeInBytes();
    final boolean mayInsertNodeMemoryReachDangerousThreshold =
        floatingMemoryUsageInByte * pipeCount >= totalFloatingMemorySizeInBytes;
    if (mayInsertNodeMemoryReachDangerousThreshold && event.mayExtractorUseTablets(this)) {
      final PipeDataNodeRemainingEventAndTimeOperator operator =
          PipeDataNodeSinglePipeMetrics.getInstance().remainingEventAndTimeOperatorMap.get(pipeID);
      LOGGER.info(
          "Pipe task {}@{} canNotUseTabletAnyMore(1) for tsFile {}: The memory usage of the insert node {} has reached the dangerous threshold of single pipe {}, event count: {}",
          pipeName,
          dataRegionId,
          event.getTsFileEpoch().getFilePath(),
          floatingMemoryUsageInByte,
          totalFloatingMemorySizeInBytes / pipeCount,
          Optional.ofNullable(operator)
              .map(PipeDataNodeRemainingEventAndTimeOperator::getInsertNodeEventCount)
              .orElse(0));
    }
    return mayInsertNodeMemoryReachDangerousThreshold;
  }

  /**
   * These judgements are deprecated, and are only reserved for manual operation and compatibility.
   */
  @Deprecated
  private boolean canNotUseTabletAnymoreDeprecated(final PipeRealtimeEvent event) {
    // In the following 5 cases, we should not extract any more tablet events. all the data
    // represented by the tablet events should be carried by the following tsfile event:
    //  1. The number of historical tsFile events to transfer has exceeded the limit.
    //  2. The number of realtime tsfile events to transfer has exceeded the limit.
    //  3. The number of linked tsFiles has reached the dangerous threshold.
    return isHistoricalTsFileEventCountExceededLimit(event)
        || isRealtimeTsFileEventCountExceededLimit(event)
        || mayTsFileLinkedCountReachDangerousThreshold(event);
  }

  private boolean isHistoricalTsFileEventCountExceededLimit(final PipeRealtimeEvent event) {
    if (PipeConfig.getInstance().getPipeMaxAllowedHistoricalTsFilePerDataRegion()
        == Integer.MAX_VALUE) {
      return false;
    }
    final IoTDBDataRegionExtractor extractor =
        PipeDataRegionExtractorMetrics.getInstance().getExtractorMap().get(getTaskID());
    final boolean isHistoricalTsFileEventCountExceededLimit =
        Objects.nonNull(extractor)
            && extractor.getHistoricalTsFileInsertionEventCount()
                >= PipeConfig.getInstance().getPipeMaxAllowedHistoricalTsFilePerDataRegion();
    if (isHistoricalTsFileEventCountExceededLimit && event.mayExtractorUseTablets(this)) {
      LOGGER.info(
          "Pipe task {}@{} canNotUseTabletAnymoreDeprecated(1) for tsFile {}: The number of historical tsFile events {} has exceeded the limit {}",
          pipeName,
          dataRegionId,
          event.getTsFileEpoch().getFilePath(),
          extractor.getHistoricalTsFileInsertionEventCount(),
          PipeConfig.getInstance().getPipeMaxAllowedHistoricalTsFilePerDataRegion());
    }
    return isHistoricalTsFileEventCountExceededLimit;
  }

  private boolean isRealtimeTsFileEventCountExceededLimit(final PipeRealtimeEvent event) {
    if (PipeConfig.getInstance().getPipeMaxAllowedPendingTsFileEpochPerDataRegion()
        == Integer.MAX_VALUE) {
      return false;
    }
    final boolean isRealtimeTsFileEventCountExceededLimit =
        pendingQueue.getTsFileInsertionEventCount()
            >= PipeConfig.getInstance().getPipeMaxAllowedPendingTsFileEpochPerDataRegion();
    if (isRealtimeTsFileEventCountExceededLimit && event.mayExtractorUseTablets(this)) {
      LOGGER.info(
          "Pipe task {}@{} canNotUseTabletAnymoreDeprecated(2) for tsFile {}: The number of realtime tsFile events {} has exceeded the limit {}",
          pipeName,
          dataRegionId,
          event.getTsFileEpoch().getFilePath(),
          pendingQueue.getTsFileInsertionEventCount(),
          PipeConfig.getInstance().getPipeMaxAllowedPendingTsFileEpochPerDataRegion());
    }
    return isRealtimeTsFileEventCountExceededLimit;
  }

  private boolean mayTsFileLinkedCountReachDangerousThreshold(final PipeRealtimeEvent event) {
    if (PipeConfig.getInstance().getPipeMaxAllowedLinkedTsFileCount() == Long.MAX_VALUE) {
      return false;
    }
    final boolean mayTsFileLinkedCountReachDangerousThreshold =
        PipeDataNodeResourceManager.tsfile().getLinkedTsFileCount(pipeName)
            >= PipeConfig.getInstance().getPipeMaxAllowedLinkedTsFileCount();
    if (mayTsFileLinkedCountReachDangerousThreshold && event.mayExtractorUseTablets(this)) {
      LOGGER.info(
          "Pipe task {}@{} canNotUseTabletAnymoreDeprecated(3) for tsFile {}: The number of linked tsFiles {} has reached the dangerous threshold {}",
          pipeName,
          dataRegionId,
          event.getTsFileEpoch().getFilePath(),
          PipeDataNodeResourceManager.tsfile().getLinkedTsFileCount(pipeName),
          PipeConfig.getInstance().getPipeMaxAllowedLinkedTsFileCount());
    }
    return mayTsFileLinkedCountReachDangerousThreshold;
  }

  @Override
  public Event supply() {
    PipeRealtimeEvent realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();

    while (realtimeEvent != null) {
      final Event suppliedEvent;

      // Used to judge the type of the event, not directly for supplying.
      final Event eventToSupply = realtimeEvent.getEvent();
      if (eventToSupply instanceof TabletInsertionEvent) {
        suppliedEvent = supplyTabletInsertion(realtimeEvent);
      } else if (eventToSupply instanceof TsFileInsertionEvent) {
        suppliedEvent = supplyTsFileInsertion(realtimeEvent);
      } else if (eventToSupply instanceof PipeHeartbeatEvent) {
        suppliedEvent = supplyHeartbeat(realtimeEvent);
      } else if (eventToSupply instanceof PipeDeleteDataNodeEvent
          || eventToSupply instanceof ProgressReportEvent) {
        suppliedEvent = supplyDirectly(realtimeEvent);
      } else {
        throw new UnsupportedOperationException(
            String.format(
                "Unsupported event type %s for hybrid realtime extractor %s to supply.",
                eventToSupply.getClass(), this));
      }

      realtimeEvent.decreaseReferenceCount(
          PipeRealtimeDataRegionHybridExtractor.class.getName(), false);

      if (suppliedEvent != null) {
        maySkipIndex4Event(realtimeEvent);
        return suppliedEvent;
      }

      realtimeEvent = (PipeRealtimeEvent) pendingQueue.directPoll();
    }

    // Means the pending queue is empty.
    return null;
  }

  private Event supplyTabletInsertion(final PipeRealtimeEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              switch (state) {
                case EMPTY:
                  return canNotUseTabletAnyMore(event)
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_TABLET;
                case USING_TSFILE:
                  return canNotUseTabletAnyMore(event)
                      ? TsFileEpoch.State.USING_TSFILE
                      : TsFileEpoch.State.USING_BOTH;
                case USING_TABLET:
                case USING_BOTH:
                default:
                  return state;
              }
            });

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    if (state == TsFileEpoch.State.USING_TSFILE) {
      PipeTsFileEpochProgressIndexKeeper.getInstance()
          .registerProgressIndex(dataRegionId, pipeName, event.getTsFileEpoch().getResource());
    }

    switch (state) {
      case USING_TSFILE:
        // If the state is USING_TSFILE, discard the event and poll the next one.
        return null;
      case EMPTY:
      case USING_TABLET:
      case USING_BOTH:
      default:
        if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName())) {
          return event.getEvent();
        } else {
          // If the event's reference count can not be increased, it means the data represented by
          // this event is not reliable anymore. but the data represented by this event
          // has been carried by the following tsfile event, so we can just discard this event.
          event.getTsFileEpoch().migrateState(this, s -> TsFileEpoch.State.USING_BOTH);
          LOGGER.warn(
              "Discard tablet event {} because it is not reliable anymore. "
                  + "Change the state of TsFileEpoch to USING_TSFILE.",
              event);
          return null;
        }
    }
  }

  private Event supplyTsFileInsertion(final PipeRealtimeEvent event) {
    event
        .getTsFileEpoch()
        .migrateState(
            this,
            state -> {
              // This would not happen, but just in case.
              if (state.equals(TsFileEpoch.State.EMPTY)) {
                LOGGER.error(
                    String.format("EMPTY TsFileEpoch when supplying TsFile Event %s", event));
                return TsFileEpoch.State.USING_TSFILE;
              }
              return state;
            });

    final TsFileEpoch.State state = event.getTsFileEpoch().getState(this);
    switch (state) {
      case USING_TABLET:
        // If the state is USING_TABLET, discard the event and poll the next one.
        PipeTsFileEpochProgressIndexKeeper.getInstance()
            .eliminateProgressIndex(dataRegionId, pipeName, event.getTsFileEpoch().getFilePath());
        return null;
      case EMPTY:
      case USING_TSFILE:
      case USING_BOTH:
      default:
        if (event.increaseReferenceCount(PipeRealtimeDataRegionHybridExtractor.class.getName())) {
          return event.getEvent();
        } else {
          // If the event's reference count can not be increased, it means the data represented by
          // this event is not reliable anymore. the data has been lost. we simply discard this
          // event
          // and report the exception to PipeRuntimeAgent.
          final String errorMessage =
              String.format(
                  "TsFile Event %s can not be supplied because "
                      + "the reference count can not be increased, "
                      + "the data represented by this event is lost",
                  event.getEvent());
          LOGGER.error(errorMessage);
          PipeDataNodeAgent.runtime()
              .report(pipeTaskMeta, new PipeRuntimeNonCriticalException(errorMessage));
          PipeTsFileEpochProgressIndexKeeper.getInstance()
              .eliminateProgressIndex(dataRegionId, pipeName, event.getTsFileEpoch().getFilePath());
          return null;
        }
    }
  }
}
