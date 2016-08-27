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

import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LuceneDruidQuery extends BaseQuery<Result<LuceneQueryResultValue>>
{

  public static final String TYPE = "lucene";
  private final String query;
  private final String defaultField;
  private final int count;

  @JsonCreator
  public LuceneDruidQuery(@JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("defaultField") String defaultField,
      @JsonProperty("query") String query, @JsonProperty("count") int count)
  {
    super(dataSource, querySegmentSpec, false, context);
    this.defaultField = defaultField;
    this.query = query;
    this.count = count;
  }

  public int getCount()
  {
    return count;
  }

  public String getDefaultField()
  {
    return defaultField;
  }

  public String getQueryString()
  {
    return query;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public Query<Result<LuceneQueryResultValue>> withOverriddenContext(
      Map<String, Object> contextOverride)
  {
    return new LuceneDruidQuery(getDataSource(), getQuerySegmentSpec(),
        computeOverridenContext(contextOverride), defaultField, query, count);
  }

  @Override
  public Query<Result<LuceneQueryResultValue>> withQuerySegmentSpec(
      QuerySegmentSpec spec)
  {
    return new LuceneDruidQuery(getDataSource(), spec, getContext(),
        defaultField, query, count);
  }

  @Override
  public Query<Result<LuceneQueryResultValue>> withDataSource(
      DataSource dataSource)
  {
    return new LuceneDruidQuery(dataSource, getQuerySegmentSpec(),
        getContext(), defaultField, query, count);
  }
}
