/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.extension.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class LuceneQueryResultValue
{
  private final long count;
  private final long size;

  @JsonCreator
  public LuceneQueryResultValue(@JsonProperty("size") long size,
      @JsonProperty("count") long count)
  {
    this.size = size;
    this.count = count;
  }

  @JsonProperty
  public long getCount()
  {
    return count;
  }

  @JsonProperty
  public long getSize()
  {
    return size;
  }

  @Override
  public String toString()
  {
    return "LuceneQueryResultValue{" + "count=" + count + ", " + "size=" + size
        + '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o)
    {
      return true;
    }
    if (o == null || getClass() != o.getClass())
    {
      return false;
    }
    LuceneQueryResultValue that = (LuceneQueryResultValue) o;
    return count == that.count;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(size, count);
  }
}
