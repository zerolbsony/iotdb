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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FrameBound extends Node {
  public enum Type {
    UNBOUNDED_PRECEDING,
    PRECEDING,
    CURRENT_ROW,
    FOLLOWING,
    UNBOUNDED_FOLLOWING
  }

  private final Type type;
  private final Optional<Expression> value;

  public FrameBound(NodeLocation location, Type type) {
    this(location, type, null);
  }

  public FrameBound(NodeLocation location, Type type, Expression value) {
    super(location);
    this.type = requireNonNull(type, "type is null");
    this.value = Optional.ofNullable(value);
  }

  public Type getType() {
    return type;
  }

  public Optional<Expression> getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFrameBound(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    value.ifPresent(nodes::add);
    return nodes.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    FrameBound o = (FrameBound) obj;
    return type == o.type && Objects.equals(value, o.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("type", type).add("value", value).toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    if (!sameClass(this, other)) {
      return false;
    }

    FrameBound otherNode = (FrameBound) other;
    return type == otherNode.type;
  }

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write((byte) type.ordinal(), buffer);
    if (value.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, buffer);
      Expression.serialize(value.get(), buffer);
    } else {
      ReadWriteIOUtils.write((byte) 0, buffer);
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write((byte) type.ordinal(), stream);
    if (value.isPresent()) {
      ReadWriteIOUtils.write((byte) 1, stream);
      Expression.serialize(value.get(), stream);
    } else {
      ReadWriteIOUtils.write((byte) 0, stream);
    }
  }

  public FrameBound(ByteBuffer byteBuffer) {
    super(null);

    type = Type.values()[ReadWriteIOUtils.readByte(byteBuffer)];
    if (ReadWriteIOUtils.readByte(byteBuffer) == 1) {
      this.value = Optional.of(Expression.deserialize(byteBuffer));
    } else {
      this.value = Optional.empty();
    }
  }
}
