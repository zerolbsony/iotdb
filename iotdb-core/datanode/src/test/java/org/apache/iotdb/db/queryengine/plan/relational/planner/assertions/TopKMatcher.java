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

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TopKNode;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.MatchResult.NO_MATCH;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.assertions.Util.orderingSchemeMatches;

public class TopKMatcher implements Matcher {
  private final List<PlanMatchPattern.Ordering> orderBy;
  private final long count;
  private final boolean childrenDataInOrder;

  public TopKMatcher(
      List<PlanMatchPattern.Ordering> orderBy, long count, boolean childrenDataInOrder) {
    this.orderBy = orderBy;
    this.count = count;
    this.childrenDataInOrder = childrenDataInOrder;
  }

  @Override
  public boolean shapeMatches(PlanNode node) {
    return node instanceof TopKNode;
  }

  @Override
  public MatchResult detailMatches(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    checkState(
        shapeMatches(node),
        "Plan testing framework error: shapeMatches returned false in detailMatches in %s",
        this.getClass().getName());
    TopKNode topKNode = (TopKNode) node;

    if (!orderingSchemeMatches(orderBy, topKNode.getOrderingScheme(), symbolAliases)
        || count != topKNode.getCount()
        || childrenDataInOrder != topKNode.isChildrenDataInOrder()) {
      return NO_MATCH;
    }

    return MatchResult.match();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("orderBy", orderBy)
        .add("count", count)
        .add("childrenDataInOrder", childrenDataInOrder)
        .toString();
  }
}
