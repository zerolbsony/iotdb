/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.window.function.value;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;

import org.apache.tsfile.block.column.ColumnBuilder;

public class FirstValueFunction extends ValueWindowFunction {
  private final int channel;
  private final boolean ignoreNull;

  public FirstValueFunction(int channel, boolean ignoreNull) {
    this.channel = channel;
    this.ignoreNull = ignoreNull;
  }

  @Override
  public void transform(
      Partition partition, ColumnBuilder builder, int index, int frameStart, int frameEnd) {
    // Empty frame
    if (frameStart < 0) {
      builder.appendNull();
      return;
    }

    if (ignoreNull) {
      // Handle nulls
      int pos = frameStart;
      while (pos <= frameEnd && partition.isNull(channel, pos)) {
        pos++;
      }

      if (pos > frameEnd) {
        builder.appendNull();
      } else {
        partition.writeTo(builder, channel, pos);
      }
    } else {
      if (partition.isNull(channel, frameStart)) {
        builder.appendNull();
      } else {
        partition.writeTo(builder, channel, frameStart);
      }
    }
  }
}
