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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;

import io.druid.common.utils.JodaUtils;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;

import javax.annotation.Nullable;

public class LuceneQueryToolChest extends
    QueryToolChest<Result<LuceneQueryResultValue>, LuceneDruidQuery>
{
  @Override
  public QueryRunner<Result<LuceneQueryResultValue>> mergeResults(
      final QueryRunner<Result<LuceneQueryResultValue>> baseRunner)

  {
    return new ResultMergeQueryRunner<Result<LuceneQueryResultValue>>(
        baseRunner)
    {
      @Override
      protected Ordering<Result<LuceneQueryResultValue>> makeOrdering(
          final Query<Result<LuceneQueryResultValue>> query)
      {
        return (Ordering) Ordering.allEqual().nullsFirst();
      }

      @Override
      protected BinaryFn<Result<LuceneQueryResultValue>, Result<LuceneQueryResultValue>, Result<LuceneQueryResultValue>> createMergeFn(
          final Query<Result<LuceneQueryResultValue>> query)
      {
        return new BinaryFn<Result<LuceneQueryResultValue>, Result<LuceneQueryResultValue>, Result<LuceneQueryResultValue>>()
        {
          @Override
          public Result<LuceneQueryResultValue> apply(
              @Nullable Result<LuceneQueryResultValue> result1,
              @Nullable Result<LuceneQueryResultValue> result2)
          {
            if (result1 == null)
            {
              return result2;
            } else if (result2 == null)
            {
              return result1;
            } else
            {
              return new Result<>(JodaUtils.minDateTime(result1.getTimestamp(),
                  result2.getTimestamp()), new LuceneQueryResultValue(result1
                  .getValue().getSize() + result1.getValue().getSize(), result1
                  .getValue().getCount() + result2.getValue().getCount()));
            }
          }
        };
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(
      final LuceneDruidQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<LuceneQueryResultValue>, Result<LuceneQueryResultValue>> makePreComputeManipulatorFn(
      final LuceneDruidQuery query, final MetricManipulationFn fn)
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<LuceneQueryResultValue>> getResultTypeReference()
  {
    return new TypeReference<Result<LuceneQueryResultValue>>()
    {
    };
  }
}
