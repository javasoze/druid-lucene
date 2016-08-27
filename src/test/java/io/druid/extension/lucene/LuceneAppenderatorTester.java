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

import io.druid.concurrent.Execs;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryWatcher;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Granularity;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;

/**
 */
public class LuceneAppenderatorTester implements AutoCloseable {
    public static final QueryWatcher NOOP_QUERYWATCHER = new QueryWatcher()
    {
        @Override
        public void registerQuery(Query query, ListenableFuture future)
        {

        }
    };

    public static final String DATASOURCE = "foo";

    private final DataSchema schema;
    private final RealtimeTuningConfig tuningConfig;
    private final DataSegmentPusher dataSegmentPusher;
    private final ObjectMapper objectMapper;
    private final Appenderator appenderator;
    private final ExecutorService queryExecutor;
    private final ServiceEmitter emitter;

    private final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<>();


    public LuceneAppenderatorTester(
            final int maxRowsInMemory
    )
    {
        this(maxRowsInMemory, null);
    }

    public LuceneAppenderatorTester(
            final int maxRowsInMemory,
            final File basePersistDirectory
    )
    {
        objectMapper = new DefaultObjectMapper();
        objectMapper.registerSubtypes(LinearShardSpec.class);

        final Map<String, Object> parserMap = objectMapper.convertValue(
                new MapInputRowParser(
                        new JSONParseSpec(
                                new TimestampSpec("ts", "auto", null),
                                new DimensionsSpec(
                                        DimensionsSpec.getDefaultSchemas(Arrays.asList("dim")),
                                        null,
                                        null)
                        )
                ),
                Map.class
        );
        schema = new DataSchema(
                DATASOURCE,
                parserMap,
                new AggregatorFactory[]{
                        new CountAggregatorFactory("count"),
                        new LongSumAggregatorFactory("met", "met")
                },
                new UniformGranularitySpec(Granularity.MINUTE, QueryGranularities.NONE, null),
                objectMapper
        );

        tuningConfig = new RealtimeTuningConfig(
                maxRowsInMemory,
                null,
                null,
                basePersistDirectory,
                null,
                null,
                null,
                null,
                null,
                null,
                0,
                0,
                null,
                null
        );

        queryExecutor = Execs.singleThreaded("queryExecutor(%d)");

        emitter = new ServiceEmitter(
                "test",
                "test",
                new LoggingEmitter(
                        new Logger(LuceneAppenderatorTester.class),
                        LoggingEmitter.Level.INFO,
                        objectMapper
                )
        );
        emitter.start();
        EmittingLogger.registerEmitter(emitter);
        dataSegmentPusher = new DataSegmentPusher()
        {
            @Override
            public String getPathForHadoop(String dataSource)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public DataSegment push(File file, DataSegment segment) throws IOException
            {
                pushedSegments.add(segment);
                return segment;
            }
        };
        appenderator = new LuceneAppenderator(
                schema,
                tuningConfig,
                new DefaultQueryRunnerFactoryConglomerate(
                        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>of(
                                LuceneDruidQuery.class, new LuceneQueryRunnerFactory(
                                        NOOP_QUERYWATCHER
                                )
                        )
                ),
                queryExecutor
        );
    }

    public DataSchema getSchema()
    {
        return schema;
    }

    public RealtimeTuningConfig getTuningConfig()
    {
        return tuningConfig;
    }

    public DataSegmentPusher getDataSegmentPusher()
    {
        return dataSegmentPusher;
    }

    public ObjectMapper getObjectMapper()
    {
        return objectMapper;
    }

    public Appenderator getAppenderator()
    {
        return appenderator;
    }

    public List<DataSegment> getPushedSegments()
    {
        return pushedSegments;
    }

    @Override
    public void close() throws Exception {

    }
}