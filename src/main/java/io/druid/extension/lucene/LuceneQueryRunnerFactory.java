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

import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.segment.Segment;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.EmittingLogger;

public class LuceneQueryRunnerFactory implements
    QueryRunnerFactory<Result<LuceneQueryResultValue>, LuceneDruidQuery>
{
  private static final EmittingLogger log = new EmittingLogger(
      LuceneQueryRunnerFactory.class);
  private final QueryWatcher watcher;

  @Inject
  public LuceneQueryRunnerFactory(QueryWatcher watcher)
  {
    this.watcher = watcher;
  }

  @Override
  public QueryRunner<Result<LuceneQueryResultValue>> createRunner(
      final Segment segment)
  {
    return new LuceneQueryRunner(segment.as(LuceneDruidSegment.class));
  }

  @Override
  public QueryRunner<Result<LuceneQueryResultValue>> mergeRunners(
      final ExecutorService queryExecutor,
      final Iterable<QueryRunner<Result<LuceneQueryResultValue>>> runners)
  {
    return new ChainedExecutionQueryRunner<Result<LuceneQueryResultValue>>(
        queryExecutor, watcher, runners);
  }

  @Override
  public LuceneQueryToolChest getToolchest()
  {
    return new LuceneQueryToolChest();
  }

  private static class LuceneQueryRunner implements
      QueryRunner<Result<LuceneQueryResultValue>>
  {
    private final LuceneDruidSegment segment;

    public LuceneQueryRunner(LuceneDruidSegment segment)
    {
      this.segment = segment;
    }

    @Override
    public Sequence<Result<LuceneQueryResultValue>> run(
        final Query<Result<LuceneQueryResultValue>> query,
        final Map<String, Object> responseContext)
    {
      log.info("here... handling run");
      LuceneDruidQuery luceneDruidQuery = (LuceneDruidQuery) query;
      long numHits = 0;

      IndexReader reader = null;
      try
      {
        reader = segment.getIndexReader();
        String queryString = luceneDruidQuery.getQueryString();
        log.info("query string: " + queryString);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser(
            luceneDruidQuery.getDefaultField(), analyzer);
        if (reader != null)
        {
          log.info("we have a reader to search with " + reader.numDocs()
              + " docs");
          org.apache.lucene.search.Query luceneQuery = (queryString == null || "*"
              .equals(queryString)) ? new MatchAllDocsQuery() : parser
              .parse(queryString);
          log.info("lucene query: " + luceneQuery);
          IndexSearcher searcher = new IndexSearcher(reader);
          TopDocs td = searcher
              .search(luceneQuery, luceneDruidQuery.getCount());
          numHits = td.totalHits;
        }
      } catch (Exception e)
      {
        log.error(e, e.getMessage());
      } finally
      {
        if (reader != null)
        {
          try
          {
            reader.close();
          } catch (IOException e)
          {
            log.error(e.getMessage(), e);
          }
        }
      }
      return Sequences.simple(ImmutableList.of(new Result<>(segment
          .getDataInterval().getStart(), new LuceneQueryResultValue(numHits,
          segment.numRows()))));
    }
  }
}
