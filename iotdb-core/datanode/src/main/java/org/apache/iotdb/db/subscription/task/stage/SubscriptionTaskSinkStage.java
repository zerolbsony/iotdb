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

package org.apache.iotdb.db.subscription.task.stage;

import org.apache.iotdb.commons.pipe.agent.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.db.pipe.agent.task.execution.PipeSinkSubtaskExecutor;
import org.apache.iotdb.db.pipe.agent.task.stage.PipeTaskSinkStage;
import org.apache.iotdb.db.subscription.task.subtask.SubscriptionSinkSubtaskManager;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.exception.PipeException;

public class SubscriptionTaskSinkStage extends PipeTaskSinkStage {

  public SubscriptionTaskSinkStage(
      String pipeName,
      long creationTime,
      PipeParameters pipeConnectorParameters,
      int regionId,
      PipeSinkSubtaskExecutor executor) {
    super(pipeName, creationTime, pipeConnectorParameters, regionId, () -> executor);
  }

  @Override
  protected void registerSubtask() {
    this.connectorSubtaskId =
        SubscriptionSinkSubtaskManager.instance()
            .register(
                executor.get(),
                pipeConnectorParameters,
                new PipeTaskSinkRuntimeEnvironment(pipeName, creationTime, regionId));
  }

  @Override
  public void createSubtask() throws PipeException {
    // Do nothing
  }

  @Override
  public void startSubtask() throws PipeException {
    SubscriptionSinkSubtaskManager.instance().start(connectorSubtaskId);
  }

  @Override
  public void stopSubtask() throws PipeException {
    SubscriptionSinkSubtaskManager.instance().stop(connectorSubtaskId);
  }

  @Override
  public void dropSubtask() throws PipeException {
    SubscriptionSinkSubtaskManager.instance()
        .deregister(pipeName, creationTime, regionId, connectorSubtaskId);
  }

  public UnboundedBlockingPendingQueue<Event> getPipeConnectorPendingQueue() {
    return SubscriptionSinkSubtaskManager.instance()
        .getPipeConnectorPendingQueue(connectorSubtaskId);
  }
}
