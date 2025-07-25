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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.load.LoadReadOnlyException;
import org.apache.iotdb.db.exception.load.RegionReplicaSetChangedException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.service.RegionMigrateService;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.ArrayDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.timeindex.PlainDeviceTimeIndex;
import org.apache.iotdb.db.storageengine.load.converter.LoadTsFileDataTypeConverter;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileDataCacheMemoryBlock;
import org.apache.iotdb.db.storageengine.load.memory.LoadTsFileMemoryManager;
import org.apache.iotdb.db.storageengine.load.metrics.LoadTsFileCostMetricsSet;
import org.apache.iotdb.db.storageengine.load.splitter.ChunkData;
import org.apache.iotdb.db.storageengine.load.splitter.DeletionData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileData;
import org.apache.iotdb.db.storageengine.load.splitter.TsFileSplitter;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.mpp.rpc.thrift.TLoadCommandReq;
import org.apache.iotdb.rpc.TSStatusCode;

import io.airlift.units.Duration;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.PublicBAOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * {@link LoadTsFileScheduler} is used for scheduling {@link LoadSingleTsFileNode} and {@link
 * LoadTsFilePieceNode}. because these two nodes need two phases to finish transfer.
 *
 * <p>for more details please check: <a
 * href="https://apache-iotdb.feishu.cn/docx/doxcnyBYWzek8ksSEU6obZMpYLe">...</a>;
 */
public class LoadTsFileScheduler implements IScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileScheduler.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private static final LoadTsFileCostMetricsSet LOAD_TSFILE_COST_METRICS_SET =
      LoadTsFileCostMetricsSet.getInstance();

  private static final long SINGLE_SCHEDULER_MAX_MEMORY_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize() >> 2;
  private static final int TRANSMIT_LIMIT =
      CommonDescriptor.getInstance().getConfig().getTTimePartitionSlotTransmitLimit();

  private static final Set<String> LOADING_FILE_SET = new HashSet<>();

  private final MPPQueryContext queryContext;
  private final QueryStateMachine stateMachine;
  private final LoadTsFileDispatcherImpl dispatcher;
  private final DataPartitionBatchFetcher partitionFetcher;
  private final List<LoadSingleTsFileNode> tsFileNodeList;
  private final List<Integer> failedTsFileNodeIndexes;
  private final PlanFragmentId fragmentId;
  private final Set<TRegionReplicaSet> allReplicaSets;
  private final boolean isGeneratedByPipe;
  private final Map<TTimePartitionSlot, ProgressIndex> timePartitionSlotToProgressIndex;
  private final LoadTsFileDataCacheMemoryBlock block;

  public LoadTsFileScheduler(
      DistributedQueryPlan distributedQueryPlan,
      MPPQueryContext queryContext,
      QueryStateMachine stateMachine,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> internalServiceClientManager,
      IPartitionFetcher partitionFetcher,
      boolean isGeneratedByPipe) {
    this.queryContext = queryContext;
    this.stateMachine = stateMachine;
    this.tsFileNodeList = new ArrayList<>();
    this.failedTsFileNodeIndexes = new ArrayList<>();
    this.fragmentId = distributedQueryPlan.getRootSubPlan().getPlanFragment().getId();
    this.dispatcher = new LoadTsFileDispatcherImpl(internalServiceClientManager, isGeneratedByPipe);
    this.partitionFetcher = new DataPartitionBatchFetcher(partitionFetcher);
    this.allReplicaSets = new HashSet<>();
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.timePartitionSlotToProgressIndex = new HashMap<>();
    this.block = LoadTsFileMemoryManager.getInstance().allocateDataCacheMemoryBlock();

    for (FragmentInstance fragmentInstance : distributedQueryPlan.getInstances()) {
      tsFileNodeList.add((LoadSingleTsFileNode) fragmentInstance.getFragment().getPlanNodeTree());
    }
  }

  @Override
  public void start() {
    try {
      stateMachine.transitionToRunning();
      int tsFileNodeListSize = tsFileNodeList.size();
      boolean isLoadSuccess = true;

      for (int i = 0; i < tsFileNodeListSize; ++i) {
        final LoadSingleTsFileNode node = tsFileNodeList.get(i);
        final String filePath = node.getTsFileResource().getTsFilePath();

        if (node.isTableModel()) {
          partitionFetcher.setDatabase(node.getDatabase());
        } else {
          partitionFetcher.setDatabase(null);
        }

        boolean isLoadSingleTsFileSuccess = true;
        boolean shouldRemoveFileFromLoadingSet = false;
        try {
          synchronized (LOADING_FILE_SET) {
            if (LOADING_FILE_SET.contains(filePath)) {
              throw new LoadFileException(
                  String.format("TsFile %s is loading by another scheduler.", filePath));
            }
            LOADING_FILE_SET.add(filePath);
          }
          shouldRemoveFileFromLoadingSet = true;

          final long startTimeMs = System.currentTimeMillis();

          if (node.isTsFileEmpty()) {
            LOGGER.info("Load skip TsFile {}, because it has no data.", filePath);
          } else if (!node.needDecodeTsFile(
              slotList ->
                  partitionFetcher.queryDataPartition(
                      slotList, queryContext.getSession().getUserName()))) {
            // do not decode, load locally
            final long startTime = System.nanoTime();
            try {
              isLoadSingleTsFileSuccess = loadLocally(node);
            } finally {
              LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
                  LoadTsFileCostMetricsSet.LOAD_LOCALLY, System.nanoTime() - startTime);
            }
          } else {
            // need decode, load locally or remotely, use two phases method
            String uuid = UUID.randomUUID().toString();
            dispatcher.setUuid(uuid);
            allReplicaSets.clear();

            long startTime = System.nanoTime();
            final boolean isFirstPhaseSuccess;
            try {
              isFirstPhaseSuccess = firstPhase(node);
            } finally {
              LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
                  LoadTsFileCostMetricsSet.FIRST_PHASE, System.nanoTime() - startTime);
            }

            startTime = System.nanoTime();
            final boolean isSecondPhaseSuccess;
            try {
              isSecondPhaseSuccess =
                  secondPhase(isFirstPhaseSuccess, uuid, node.getTsFileResource());
            } finally {
              LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
                  LoadTsFileCostMetricsSet.SECOND_PHASE, System.nanoTime() - startTime);
            }

            if (!isFirstPhaseSuccess || !isSecondPhaseSuccess) {
              isLoadSingleTsFileSuccess = false;
            }
          }

          if (RegionMigrateService.getInstance().getLastNotifyMigratingTime() > startTimeMs
              || RegionMigrateService.getInstance().mayHaveMigratingRegions()) {
            LOGGER.warn(
                "LoadTsFileScheduler: Region migration was detected during loading TsFile {}, will convert to insertion to avoid data loss",
                filePath);
            isLoadSingleTsFileSuccess = false;
          }

          if (isLoadSingleTsFileSuccess) {
            node.clean();
            LOGGER.info(
                "Load TsFile {} Successfully, load process [{}/{}]",
                filePath,
                i + 1,
                tsFileNodeListSize);
          } else {
            isLoadSuccess = false;
            failedTsFileNodeIndexes.add(i);
            LOGGER.warn(
                "Can not Load TsFile {}, load process [{}/{}]",
                filePath,
                i + 1,
                tsFileNodeListSize);
          }
        } catch (Exception e) {
          isLoadSuccess = false;
          failedTsFileNodeIndexes.add(i);
          LOGGER.warn("LoadTsFileScheduler loads TsFile {} error", filePath, e);
        } finally {
          if (shouldRemoveFileFromLoadingSet) {
            synchronized (LOADING_FILE_SET) {
              LOADING_FILE_SET.remove(filePath);
            }
          }
        }
      }

      if (isLoadSuccess) {
        stateMachine.transitionToFinished();
      } else {
        final StringBuilder failedTsFiles =
            new StringBuilder(
                !tsFileNodeList.isEmpty()
                    ? tsFileNodeList.get(0).getTsFileResource().getTsFilePath()
                    : "");
        final ListIterator<Integer> iterator = failedTsFileNodeIndexes.listIterator(1);
        while (iterator.hasNext()) {
          failedTsFiles
              .append(", ")
              .append(tsFileNodeList.get(iterator.next()).getTsFileResource().getTsFilePath());
        }
        final long startTime = System.nanoTime();
        try {
          // if failed to load some TsFiles, then try to convert the TsFiles to Tablets
          LOGGER.info(
              "Load TsFile(s) failed, will try to convert to tablets and insert. Failed TsFiles: {}",
              failedTsFiles);
          convertFailedTsFilesToTabletsAndRetry();
        } finally {
          LOAD_TSFILE_COST_METRICS_SET.recordPhaseTimeCost(
              LoadTsFileCostMetricsSet.SCHEDULER_CAST_TABLETS, System.nanoTime() - startTime);
        }
      }
    } finally {
      LoadTsFileMemoryManager.getInstance().releaseDataCacheMemoryBlock();
    }
  }

  private boolean firstPhase(LoadSingleTsFileNode node) {
    final TsFileDataManager tsFileDataManager = new TsFileDataManager(this, node, block);
    try {
      new TsFileSplitter(
              node.getTsFileResource().getTsFile(), tsFileDataManager::addOrSendTsFileData)
          .splitTsFileByDataPartition();
      if (!tsFileDataManager.sendAllTsFileData()) {
        return false;
      }
    } catch (IllegalStateException e) {
      LOGGER.warn(
          String.format(
              "Dispatch TsFileData error when parsing TsFile %s.",
              node.getTsFileResource().getTsFile()),
          e);
      return false;
    } catch (Exception e) {
      LOGGER.warn(
          String.format("Parse or send TsFile %s error.", node.getTsFileResource().getTsFile()), e);
      return false;
    } finally {
      tsFileDataManager.clear();
    }
    return true;
  }

  private boolean dispatchOnePieceNode(
      LoadTsFilePieceNode pieceNode, TRegionReplicaSet replicaSet) {
    allReplicaSets.add(replicaSet);
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(fragmentId, pieceNode),
            fragmentId.genFragmentInstanceId(),
            null,
            queryContext.getQueryType(),
            queryContext.getTimeOut() - (System.currentTimeMillis() - queryContext.getStartTime()),
            queryContext.getSession());
    instance.setExecutorAndHost(new StorageExecutor(replicaSet));
    Future<FragInstanceDispatchResult> dispatchResultFuture =
        dispatcher.dispatch(null, Collections.singletonList(instance));

    try {
      FragInstanceDispatchResult result =
          dispatchResultFuture.get(
              CONFIG.getLoadCleanupTaskExecutionDelayTimeSeconds(), TimeUnit.SECONDS);
      if (!result.isSuccessful()) {
        LOGGER.warn(
            "Dispatch one piece to ReplicaSet {} error. Result status code {}. "
                + "Result status message {}. Dispatch piece node error:%n{}",
            replicaSet,
            TSStatusCode.representOf(result.getFailureStatus().getCode()).name(),
            result.getFailureStatus().getMessage(),
            pieceNode);
        if (result.getFailureStatus().getSubStatus() != null) {
          for (TSStatus status : result.getFailureStatus().getSubStatus()) {
            LOGGER.warn(
                "Sub status code {}. Sub status message {}.",
                TSStatusCode.representOf(status.getCode()).toString(),
                status.getMessage());
          }
        }
        TSStatus status = result.getFailureStatus();
        status.setMessage(
            String.format("Load %s piece error in 1st phase. Because ", pieceNode.getTsFile())
                + status.getMessage());
        return false;
      }
    } catch (InterruptedException | ExecutionException | CancellationException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.warn("Interrupt or Execution error.", e);
      return false;
    } catch (TimeoutException e) {
      dispatchResultFuture.cancel(true);
      LOGGER.warn(
          String.format("Wait for loading %s time out.", LoadTsFilePieceNode.class.getName()), e);
      return false;
    }
    return true;
  }

  private boolean secondPhase(
      boolean isFirstPhaseSuccess, String uuid, TsFileResource tsFileResource) {
    LOGGER.info("Start dispatching Load command for uuid {}", uuid);
    final File tsFile = tsFileResource.getTsFile();
    final TLoadCommandReq loadCommandReq =
        new TLoadCommandReq(
            (isFirstPhaseSuccess ? LoadCommand.EXECUTE : LoadCommand.ROLLBACK).ordinal(), uuid);

    try {
      loadCommandReq.setIsGeneratedByPipe(isGeneratedByPipe);
      loadCommandReq.setTimePartition2ProgressIndex(
          timePartitionSlotToProgressIndex.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      Map.Entry::getKey,
                      entry -> {
                        try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
                            final DataOutputStream dataOutputStream =
                                new DataOutputStream(byteArrayOutputStream)) {
                          entry.getValue().serialize(dataOutputStream);
                          return ByteBuffer.wrap(
                              byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
                        } catch (final IOException e) {
                          throw new RuntimeException(
                              String.format(
                                  "Serialize Progress Index error, isFirstPhaseSuccess: %s, uuid: %s, tsFile: %s",
                                  isFirstPhaseSuccess, uuid, tsFile.getAbsolutePath()),
                              e);
                        }
                      })));
      Future<FragInstanceDispatchResult> dispatchResultFuture =
          dispatcher.dispatchCommand(loadCommandReq, allReplicaSets);

      FragInstanceDispatchResult result = dispatchResultFuture.get();
      if (!result.isSuccessful()) {
        LOGGER.warn(
            "Dispatch load command {} of TsFile {} error to replicaSets {} error. "
                + "Result status code {}. Result status message {}.",
            loadCommandReq,
            tsFile,
            allReplicaSets,
            TSStatusCode.representOf(result.getFailureStatus().getCode()).name(),
            result.getFailureStatus().getMessage());
        TSStatus status = result.getFailureStatus();
        status.setMessage(
            String.format(
                "Load %s error in second phase. Because %s, first phase is %s",
                tsFile, status.getMessage(), isFirstPhaseSuccess ? "success" : "failed"));
        return false;
      }
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      LOGGER.warn("Interrupt or Execution error.", e);
      return false;
    } catch (Exception e) {
      LOGGER.warn("Exception occurred during second phase of loading TsFile {}.", tsFile, e);
      return false;
    }
    return true;
  }

  private ByteBuffer assignProgressIndex(TsFileResource tsFileResource) throws IOException {
    PipeDataNodeAgent.runtime().assignProgressIndexForTsFileLoad(tsFileResource);

    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      tsFileResource.getMaxProgressIndex().serialize(dataOutputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  private boolean loadLocally(LoadSingleTsFileNode node) throws IoTDBException {
    LOGGER.info("Start load TsFile {} locally.", node.getTsFileResource().getTsFile().getPath());

    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new LoadReadOnlyException();
    }

    // if the time index is PlainDeviceTimeIndex, convert it to ArrayDeviceTimeIndex
    if (node.getTsFileResource().getTimeIndex() instanceof PlainDeviceTimeIndex) {
      final PlainDeviceTimeIndex timeIndex =
          (PlainDeviceTimeIndex) node.getTsFileResource().getTimeIndex();
      final Map<IDeviceID, Integer> convertedDeviceToIndex = new ConcurrentHashMap<>();
      for (final Map.Entry<IDeviceID, Integer> entry : timeIndex.getDeviceToIndex().entrySet()) {
        convertedDeviceToIndex.put(
            entry.getKey() instanceof StringArrayDeviceID
                ? entry.getKey()
                : new StringArrayDeviceID(entry.getKey().toString()),
            entry.getValue());
      }
      node.getTsFileResource()
          .setTimeIndex(
              new ArrayDeviceTimeIndex(
                  convertedDeviceToIndex, timeIndex.getStartTimes(), timeIndex.getEndTimes()));
    }

    try {
      FragmentInstance instance =
          new FragmentInstance(
              new PlanFragment(fragmentId, node),
              fragmentId.genFragmentInstanceId(),
              null,
              queryContext.getQueryType(),
              queryContext.getTimeOut()
                  - (System.currentTimeMillis() - queryContext.getStartTime()),
              queryContext.getSession());
      instance.setExecutorAndHost(new StorageExecutor(node.getLocalRegionReplicaSet()));
      dispatcher.dispatchLocally(instance);
    } catch (FragmentInstanceDispatchException e) {
      LOGGER.warn(
          String.format(
              "Dispatch tsFile %s error to local error. Result status code %s. "
                  + "Result status message %s.",
              node.getTsFileResource().getTsFile(),
              TSStatusCode.representOf(e.getFailureStatus().getCode()).name(),
              e.getFailureStatus().getMessage()));
      return false;
    }

    // add metrics
    Optional.ofNullable(
            StorageEngine.getInstance()
                .getDataRegion(
                    (DataRegionId)
                        ConsensusGroupId.Factory.createFromTConsensusGroupId(
                            node.getLocalRegionReplicaSet().getRegionId())))
        .ifPresent(
            dataRegion ->
                dataRegion
                    .getNonSystemDatabaseName()
                    .ifPresent(
                        databaseName -> {
                          // Report load tsFile points to IoTDB flush metrics
                          MemTableFlushTask.recordFlushPointsMetricInternal(
                              node.getWritePointCount(),
                              databaseName,
                              dataRegion.getDataRegionId());

                          MetricService.getInstance()
                              .count(
                                  node.getWritePointCount(),
                                  Metric.QUANTITY.toString(),
                                  MetricLevel.CORE,
                                  Tag.NAME.toString(),
                                  Metric.POINTS_IN.toString(),
                                  Tag.DATABASE.toString(),
                                  databaseName,
                                  Tag.REGION.toString(),
                                  dataRegion.getDataRegionId(),
                                  Tag.TYPE.toString(),
                                  Metric.LOAD_POINT_COUNT.toString());
                          MetricService.getInstance()
                              .count(
                                  node.getWritePointCount(),
                                  Metric.LEADER_QUANTITY.toString(),
                                  MetricLevel.CORE,
                                  Tag.NAME.toString(),
                                  Metric.POINTS_IN.toString(),
                                  Tag.DATABASE.toString(),
                                  databaseName,
                                  Tag.REGION.toString(),
                                  dataRegion.getDataRegionId(),
                                  Tag.TYPE.toString(),
                                  Metric.LOAD_POINT_COUNT.toString());
                        }));

    return true;
  }

  private void convertFailedTsFilesToTabletsAndRetry() {
    final LoadTsFileDataTypeConverter loadTsFileDataTypeConverter =
        new LoadTsFileDataTypeConverter(queryContext, isGeneratedByPipe);

    final Iterator<Integer> iterator = failedTsFileNodeIndexes.listIterator();
    while (iterator.hasNext()) {
      final int failedLoadTsFileIndex = iterator.next();
      final LoadSingleTsFileNode failedNode = tsFileNodeList.get(failedLoadTsFileIndex);
      final String filePath = failedNode.getTsFileResource().getTsFilePath();

      try {
        final TSStatus status =
            failedNode.isTableModel()
                ? loadTsFileDataTypeConverter
                    .convertForTableModel(
                        new LoadTsFile(null, filePath, Collections.emptyMap())
                            .setDatabase(failedNode.getDatabase())
                            .setDeleteAfterLoad(failedNode.isDeleteAfterLoad())
                            .setConvertOnTypeMismatch(true))
                    .orElse(null)
                : loadTsFileDataTypeConverter
                    .convertForTreeModel(
                        new LoadTsFileStatement(filePath)
                            .setDeleteAfterLoad(failedNode.isDeleteAfterLoad())
                            .setConvertOnTypeMismatch(true))
                    .orElse(null);

        if (loadTsFileDataTypeConverter.isSuccessful(status)) {
          iterator.remove();
          LOGGER.info(
              "Load: Successfully converted TsFile {} into tablets and inserted.",
              failedNode.getTsFileResource().getTsFilePath());
        } else {
          LOGGER.warn(
              "Load: Failed to convert to tablets from TsFile {}. Status: {}",
              failedNode.getTsFileResource().getTsFilePath(),
              status);
        }
      } catch (final Exception e) {
        LOGGER.warn(
            "Load: Failed to convert to tablets from TsFile {}. Exception: {}",
            failedNode.getTsFileResource().getTsFilePath(),
            e.getMessage(),
            e);
      }
    }

    // If all failed TsFiles are converted into tablets and inserted,
    // we can consider the load process as successful.
    if (failedTsFileNodeIndexes.isEmpty()) {
      LOGGER.info("Load: all failed TsFiles are converted to tablets and inserted.");
      stateMachine.transitionToFinished();
    } else {
      final String errorMsg =
          "Load: failed to load some TsFiles by converting them into tablets. Failed TsFiles: "
              + failedTsFileNodeIndexes.stream()
                  .map(i -> tsFileNodeList.get(i).getTsFileResource().getTsFilePath())
                  .collect(Collectors.joining(", "));
      LOGGER.warn(errorMsg);
      stateMachine.transitionToFailed(new LoadFileException(errorMsg));
    }
  }

  @Override
  public void stop(Throwable t) {
    // Do nothing
  }

  @Override
  public Duration getTotalCpuTime() {
    return null;
  }

  @Override
  public FragmentInfo getFragmentInfo() {
    return null;
  }

  private void computeTimePartitionSlotToProgressIndexIfAbsent(
      final TTimePartitionSlot timePartitionSlot) {
    timePartitionSlotToProgressIndex.putIfAbsent(
        timePartitionSlot, PipeDataNodeAgent.runtime().getNextProgressIndexForTsFileLoad());
  }

  public enum LoadCommand {
    EXECUTE,
    ROLLBACK
  }

  private static class TsFileDataManager {
    private final LoadTsFileScheduler scheduler;
    private final LoadSingleTsFileNode singleTsFileNode;

    private long dataSize;
    private final Map<TConsensusGroupId, Pair<TRegionReplicaSet, LoadTsFilePieceNode>>
        regionId2ReplicaSetAndNode;
    private final List<ChunkData> nonDirectionalChunkData;
    private final LoadTsFileDataCacheMemoryBlock block;

    public TsFileDataManager(
        LoadTsFileScheduler scheduler,
        LoadSingleTsFileNode singleTsFileNode,
        LoadTsFileDataCacheMemoryBlock block) {
      this.scheduler = scheduler;
      this.singleTsFileNode = singleTsFileNode;
      this.dataSize = 0;
      this.regionId2ReplicaSetAndNode = new HashMap<>();
      this.nonDirectionalChunkData = new ArrayList<>();
      this.block = block;
    }

    private boolean addOrSendTsFileData(TsFileData tsFileData) throws LoadFileException {
      switch (tsFileData.getType()) {
        case CHUNK:
          return addOrSendChunkData((ChunkData) tsFileData);
        case DELETION:
          return addOrSendDeletionData((DeletionData) tsFileData);
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported TsFileDataType %s.", tsFileData.getType()));
      }
    }

    private boolean isMemoryEnough() {
      return dataSize <= SINGLE_SCHEDULER_MAX_MEMORY_SIZE && block.hasEnoughMemory();
    }

    private boolean addOrSendChunkData(ChunkData chunkData) throws LoadFileException {
      nonDirectionalChunkData.add(chunkData);
      dataSize += chunkData.getDataSize();
      block.addMemoryUsage(chunkData.getDataSize());
      scheduler.computeTimePartitionSlotToProgressIndexIfAbsent(chunkData.getTimePartitionSlot());

      if (!isMemoryEnough()) {
        routeChunkData();

        // start to dispatch from the biggest TsFilePieceNode
        List<TConsensusGroupId> sortedRegionIds =
            regionId2ReplicaSetAndNode.keySet().stream()
                .sorted(
                    Comparator.comparingLong(
                            o -> regionId2ReplicaSetAndNode.get(o).getRight().getDataSize())
                        .reversed())
                .collect(Collectors.toList());

        for (TConsensusGroupId sortedRegionId : sortedRegionIds) {
          final TRegionReplicaSet replicaSet =
              regionId2ReplicaSetAndNode.get(sortedRegionId).getLeft();
          final LoadTsFilePieceNode pieceNode =
              regionId2ReplicaSetAndNode.get(sortedRegionId).getRight();
          if (pieceNode.getDataSize() == 0) { // total data size has been reduced to 0
            break;
          }
          final boolean isDispatchSuccess = scheduler.dispatchOnePieceNode(pieceNode, replicaSet);

          regionId2ReplicaSetAndNode.replace(
              sortedRegionId,
              new Pair<>(
                  replicaSet,
                  new LoadTsFilePieceNode(
                      singleTsFileNode.getPlanNodeId(),
                      singleTsFileNode
                          .getTsFileResource()
                          .getTsFile()))); // can not just remove, because of deletion
          dataSize -= pieceNode.getDataSize();
          block.reduceMemoryUsage(pieceNode.getDataSize());

          if (!isDispatchSuccess) {
            // Currently there is no retry, so return directly
            return false;
          }

          if (isMemoryEnough()) {
            break;
          }
        }
      }

      return true;
    }

    private void routeChunkData() throws LoadFileException {
      if (nonDirectionalChunkData.isEmpty()) {
        return;
      }

      List<TRegionReplicaSet> replicaSets =
          scheduler.partitionFetcher.queryDataPartition(
              nonDirectionalChunkData.stream()
                  .map(data -> new Pair<>(data.getDevice(), data.getTimePartitionSlot()))
                  .collect(Collectors.toList()),
              scheduler.queryContext.getSession().getUserName());
      for (int i = 0; i < replicaSets.size(); i++) {
        final TRegionReplicaSet replicaSet = replicaSets.get(i);
        final TConsensusGroupId regionId = replicaSet.getRegionId();
        if (regionId2ReplicaSetAndNode.containsKey(regionId)
            && !Objects.equals(regionId2ReplicaSetAndNode.get(regionId).getLeft(), replicaSet)) {
          // Detected region replica set changed (maybe due to region migration), throw an exception
          throw new RegionReplicaSetChangedException(
              regionId2ReplicaSetAndNode.get(regionId).getLeft(), replicaSet);
        }

        regionId2ReplicaSetAndNode
            .computeIfAbsent(
                replicaSet.getRegionId(),
                o ->
                    new Pair<>(
                        replicaSet,
                        new LoadTsFilePieceNode(
                            singleTsFileNode.getPlanNodeId(),
                            singleTsFileNode.getTsFileResource().getTsFile())))
            .getRight()
            .addTsFileData(nonDirectionalChunkData.get(i));
      }
      nonDirectionalChunkData.clear();
    }

    private boolean addOrSendDeletionData(DeletionData deletionData) throws LoadFileException {
      routeChunkData(); // ensure chunk data will be added before deletion

      for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
          regionId2ReplicaSetAndNode.entrySet()) {
        dataSize += deletionData.getDataSize();
        block.addMemoryUsage(deletionData.getDataSize());
        entry.getValue().getRight().addTsFileData(deletionData);
      }
      return true;
    }

    private boolean sendAllTsFileData() throws LoadFileException {
      routeChunkData();

      boolean isAllSuccess = true;
      for (Map.Entry<TConsensusGroupId, Pair<TRegionReplicaSet, LoadTsFilePieceNode>> entry :
          regionId2ReplicaSetAndNode.entrySet()) {
        block.reduceMemoryUsage(entry.getValue().getRight().getDataSize());
        if (isAllSuccess
            && !scheduler.dispatchOnePieceNode(
                entry.getValue().getRight(), entry.getValue().getLeft())) {
          LOGGER.warn(
              "Dispatch piece node {} of TsFile {} error.",
              entry.getValue(),
              singleTsFileNode.getTsFileResource().getTsFile());
          isAllSuccess = false;
        }
      }
      return isAllSuccess;
    }

    private void clear() {
      regionId2ReplicaSetAndNode.clear();
    }
  }

  private static class DataPartitionBatchFetcher {
    private final IPartitionFetcher fetcher;
    private String database;

    public DataPartitionBatchFetcher(IPartitionFetcher fetcher) {
      this.fetcher = fetcher;
    }

    public void setDatabase(String database) {
      this.database = database;
    }

    public List<TRegionReplicaSet> queryDataPartition(
        List<Pair<IDeviceID, TTimePartitionSlot>> slotList, String userName) {
      List<TRegionReplicaSet> replicaSets = new ArrayList<>();
      int size = slotList.size();

      for (int i = 0; i < size; i += TRANSMIT_LIMIT) {
        List<Pair<IDeviceID, TTimePartitionSlot>> subSlotList =
            slotList.subList(i, Math.min(size, i + TRANSMIT_LIMIT));
        DataPartition dataPartition =
            fetcher.getOrCreateDataPartition(toQueryParam(subSlotList), userName);
        replicaSets.addAll(
            subSlotList.stream()
                .map(
                    pair ->
                        // (database != null) means this file will be loaded into table-model
                        database != null
                            ? dataPartition.getDataRegionReplicaSetForWriting(
                                pair.left, pair.right, database)
                            : dataPartition.getDataRegionReplicaSetForWriting(
                                pair.left, pair.right))
                .collect(Collectors.toList()));
      }
      return replicaSets;
    }

    private List<DataPartitionQueryParam> toQueryParam(
        List<Pair<IDeviceID, TTimePartitionSlot>> slots) {
      return slots.stream()
          .collect(
              Collectors.groupingBy(
                  Pair::getLeft, Collectors.mapping(Pair::getRight, Collectors.toSet())))
          .entrySet()
          .stream()
          .map(
              entry -> {
                DataPartitionQueryParam queryParam =
                    new DataPartitionQueryParam(entry.getKey(), new ArrayList<>(entry.getValue()));
                // (database != null) means this file will be loaded into table-model
                if (database != null) {
                  queryParam.setDatabaseName(database);
                }
                return queryParam;
              })
          .collect(Collectors.toList());
    }
  }
}
