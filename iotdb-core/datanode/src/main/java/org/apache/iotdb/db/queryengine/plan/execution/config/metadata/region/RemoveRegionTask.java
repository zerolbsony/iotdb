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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.RemoveRegion;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.RemoveRegionStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class RemoveRegionTask implements IConfigTask {

  protected final RemoveRegionStatement statement;
  private final Model model;

  public RemoveRegionTask(RemoveRegionStatement statement) {
    this.statement = statement;
    this.model = Model.TREE;
  }

  public RemoveRegionTask(RemoveRegion removeRegion) {
    this.statement =
        new RemoveRegionStatement(removeRegion.getRegionId(), removeRegion.getDataNodeId());
    this.model = Model.TABLE;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.removeRegion(this);
  }

  public RemoveRegionStatement getStatement() {
    return statement;
  }

  public Model getModel() {
    return model;
  }
}
