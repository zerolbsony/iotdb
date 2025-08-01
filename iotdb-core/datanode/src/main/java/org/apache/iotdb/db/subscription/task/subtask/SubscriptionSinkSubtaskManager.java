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

package org.apache.iotdb.db.subscription.task.subtask;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.agent.task.progress.PipeEventCommitManager;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.config.plugin.configuraion.PipeTaskRuntimeConfiguration;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeRealtimePriorityBlockingQueue;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtask;
import org.apache.iotdb.db.pipe.agent.task.subtask.sink.PipeSinkSubtaskLifeCycle;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionEventCounter;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin.SUBSCRIPTION_SINK;

public class SubscriptionSinkSubtaskManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionSinkSubtaskManager.class);

  private static final String FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE =
      "Failed to deregister PipeConnectorSubtask. No such subtask: ";

  // one SubscriptionConnectorSubtask for one subscription (one consumer group with one topic)
  private final Map<String, PipeSinkSubtaskLifeCycle> attributeSortedString2SubtaskLifeCycleMap =
      new HashMap<>();

  public synchronized String register(
      final PipeSinkSubtaskExecutor executor,
      final PipeParameters pipeConnectorParameters,
      final PipeTaskSinkRuntimeEnvironment environment) {
    final String connectorKey =
        pipeConnectorParameters
            .getStringOrDefault(
                Arrays.asList(PipeSinkConstant.CONNECTOR_KEY, PipeSinkConstant.SINK_KEY),
                BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            // Convert the value of `CONNECTOR_KEY` or `SINK_KEY` to lowercase
            // for matching in `CONNECTOR_CONSTRUCTORS`
            .toLowerCase();
    if (!SUBSCRIPTION_SINK.getPipePluginName().equals(connectorKey)) {
      throw new SubscriptionException(
          "The SubscriptionConnectorSubtaskManager only supports subscription-sink.");
    }

    PipeEventCommitManager.getInstance()
        .register(
            environment.getPipeName(),
            environment.getCreationTime(),
            environment.getRegionId(),
            connectorKey);

    boolean realTimeFirst =
        pipeConnectorParameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY,
                PipeSinkConstant.SINK_REALTIME_FIRST_KEY),
            PipeSinkConstant.CONNECTOR_REALTIME_FIRST_DEFAULT_VALUE);
    String attributeSortedString = generateAttributeSortedString(pipeConnectorParameters);
    attributeSortedString = "__subscription_" + attributeSortedString;

    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      final UnboundedBlockingPendingQueue<Event> pendingQueue =
          realTimeFirst
              ? new PipeRealtimePriorityBlockingQueue()
              : new UnboundedBlockingPendingQueue<>(new PipeDataRegionEventCounter());

      final PipeConnector pipeConnector =
          PipeDataNodeAgent.plugin().dataRegion().reflectConnector(pipeConnectorParameters);
      // 1. Construct, validate and customize PipeConnector, and then handshake (create connection)
      // with the target
      try {
        pipeConnector.validate(new PipeParameterValidator(pipeConnectorParameters));
        pipeConnector.customize(
            pipeConnectorParameters, new PipeTaskRuntimeConfiguration(environment));
        pipeConnector.handshake();
      } catch (final Exception e) {
        try {
          pipeConnector.close();
        } catch (final Exception closeException) {
          LOGGER.warn(
              "Failed to close connector after failed to initialize connector. "
                  + "Ignore this exception.",
              closeException);
        }
        throw new PipeException(
            "Failed to construct PipeConnector, because of " + e.getMessage(), e);
      }

      // 2. Fetch topic and consumer group id from connector parameters
      final String topicName = pipeConnectorParameters.getString(PipeSinkConstant.SINK_TOPIC_KEY);
      final String consumerGroupId =
          pipeConnectorParameters.getString(PipeSinkConstant.SINK_CONSUMER_GROUP_KEY);
      if (Objects.isNull(topicName) || Objects.isNull(consumerGroupId)) {
        throw new SubscriptionException(
            String.format(
                "Failed to construct subscription connector, because of %s or %s does not exist in pipe connector parameters",
                PipeSinkConstant.SINK_TOPIC_KEY, PipeSinkConstant.SINK_CONSUMER_GROUP_KEY));
      }

      // 3. Construct PipeConnectorSubtaskLifeCycle to manage PipeConnectorSubtask's life cycle
      final PipeSinkSubtask subtask =
          new SubscriptionSinkSubtask(
              String.format("%s_%s", attributeSortedString, environment.getCreationTime()),
              environment.getCreationTime(),
              attributeSortedString,
              0,
              pendingQueue,
              pipeConnector,
              topicName,
              consumerGroupId);
      final PipeSinkSubtaskLifeCycle pipeSinkSubtaskLifeCycle =
          new SubscriptionSinkSubtaskLifeCycle(executor, subtask, pendingQueue);

      attributeSortedString2SubtaskLifeCycleMap.put(
          attributeSortedString, pipeSinkSubtaskLifeCycle);
    }

    final PipeSinkSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycle.register();

    return attributeSortedString;
  }

  public synchronized void deregister(
      final String pipeName,
      final long creationTime,
      final int regionId,
      final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    final PipeSinkSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    if (lifeCycle.deregister(pipeName, regionId)) {
      attributeSortedString2SubtaskLifeCycleMap.remove(attributeSortedString);
    }

    PipeEventCommitManager.getInstance().deregister(pipeName, creationTime, regionId);
  }

  public synchronized void start(final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    final PipeSinkSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycle.start();
  }

  public synchronized void stop(final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(FAILED_TO_DEREGISTER_EXCEPTION_MESSAGE + attributeSortedString);
    }

    final PipeSinkSubtaskLifeCycle lifeCycle =
        attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString);
    lifeCycle.stop();
  }

  public UnboundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue(
      final String attributeSortedString) {
    if (!attributeSortedString2SubtaskLifeCycleMap.containsKey(attributeSortedString)) {
      throw new PipeException(
          "Failed to get PendingQueue. No such subtask: " + attributeSortedString);
    }

    return attributeSortedString2SubtaskLifeCycleMap.get(attributeSortedString).getPendingQueue();
  }

  private String generateAttributeSortedString(final PipeParameters pipeConnectorParameters) {
    final TreeMap<String, String> sortedStringSourceMap =
        new TreeMap<>(pipeConnectorParameters.getAttribute());
    sortedStringSourceMap.remove(SystemConstant.RESTART_KEY);
    return sortedStringSourceMap.toString();
  }

  /////////////////////////  Singleton Instance Holder  /////////////////////////

  private SubscriptionSinkSubtaskManager() {
    // Do nothing
  }

  private static class SubscriptionSinkSubtaskManagerHolder {
    private static final SubscriptionSinkSubtaskManager INSTANCE =
        new SubscriptionSinkSubtaskManager();
  }

  public static SubscriptionSinkSubtaskManager instance() {
    return SubscriptionSinkSubtaskManagerHolder.INSTANCE;
  }
}
