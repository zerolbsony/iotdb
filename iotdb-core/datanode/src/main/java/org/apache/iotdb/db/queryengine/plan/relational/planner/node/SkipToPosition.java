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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum SkipToPosition {
  PAST_LAST,
  NEXT,
  FIRST,
  LAST;

  public static void serialize(SkipToPosition position, ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(position.ordinal(), byteBuffer);
  }

  public static void serialize(SkipToPosition position, DataOutputStream stream)
      throws IOException {
    ReadWriteIOUtils.write(position.ordinal(), stream);
  }

  public static SkipToPosition deserialize(ByteBuffer byteBuffer) {
    int ordinal = ReadWriteIOUtils.readInt(byteBuffer);
    return SkipToPosition.values()[ordinal];
  }
}
