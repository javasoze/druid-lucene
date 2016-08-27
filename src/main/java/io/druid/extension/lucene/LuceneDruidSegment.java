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

import com.google.common.collect.Lists;
import com.metamx.emitter.EmittingLogger;

import io.druid.data.input.InputRow;
import io.druid.segment.QueryableIndex;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LuceneDruidSegment implements Segment
{
  private static final EmittingLogger log = new EmittingLogger(
      LuceneDruidSegment.class);
  private final SegmentIdentifier segmentIdentifier;
  private final LuceneDocumentBuilder docBuilder;
  private final int maxRowsPerSegment;
  private final File persistDir;
  private int numRowsAdded;
  private RAMDirectory ramDir;
  private IndexWriter ramWriter;
  private int numRowsPersisted;
  private volatile DirectoryReader realtimeReader;
  private volatile boolean isOpen;
  private final Object refreshLock = new Object();
  private ConcurrentLinkedQueue<DirectoryReader> persistedReaders = new ConcurrentLinkedQueue<DirectoryReader>();

  private static IndexWriter buildRamWriter(RAMDirectory dir,
      Analyzer analyzer, int maxDocsPerSegment) throws IOException
  {
    IndexWriterConfig writerConfig = new IndexWriterConfig(analyzer);
    writerConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
    // some arbitrary large numbers
    writerConfig.setMaxBufferedDocs(maxDocsPerSegment * 2);
    writerConfig.setRAMBufferSizeMB(5000);
    writerConfig.setUseCompoundFile(false);
    writerConfig.setCommitOnClose(true);
    writerConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
    return new IndexWriter(dir, writerConfig);
  }

  private static IndexWriter buildPersistWriter(Directory dir)
      throws IOException
  {
    IndexWriterConfig writerConfig = new IndexWriterConfig(null);
    writerConfig.setUseCompoundFile(false);
    writerConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
    writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
    return new IndexWriter(dir, writerConfig);
  }

  private static File indexDir(File basePersistDir)
  {
    return new File(basePersistDir, UUID.randomUUID().toString());
  }

  public LuceneDruidSegment(SegmentIdentifier segmentIdentifier,
      File basePersistDir, LuceneDocumentBuilder docBuilder,
      int maxDocsPerSegment) throws IOException
  {
    this.segmentIdentifier = segmentIdentifier;
    this.persistDir = new File(basePersistDir,
        segmentIdentifier.getIdentifierAsString());
    this.docBuilder = docBuilder;
    this.maxRowsPerSegment = maxDocsPerSegment;
    this.numRowsPersisted = 0;
    this.isOpen = true;
    reset();
  }

  private void ensureOpen()
  {
    if (!isOpen)
    {
      throw new AlreadyClosedException("segment already closed");
    }
  }

  private void reset() throws IOException
  {
    if (!isOpen)
    {
      return;
    }
    ramDir = new RAMDirectory();
    ramWriter = buildRamWriter(ramDir, new StandardAnalyzer(),
        maxRowsPerSegment);
    numRowsAdded = 0;
    this.realtimeReader = null;
  }

  /**
   * Inserts a row into the index. This method is synchronized on persist by the
   * index refresh thread.
   *
   * @param row
   * @throws IOException
   */
  public void add(InputRow row) throws IOException
  {
    ensureOpen();
    Document doc = docBuilder.buildLuceneDocument(row);
    ramWriter.addDocument(doc);
    numRowsAdded++;
    if (numRowsAdded >= maxRowsPerSegment)
    {
      persist();
      reset();
    }
  }

  /**
   * Refreshes the internal index reader on a scheduled thread. This is a
   * controlled thread so it is ok to block this thread to make sure things are
   * in a good state.
   *
   * @throws IOException
   */
  public void refreshRealtimeReader() throws IOException
  {
    synchronized (refreshLock)
    {
      if (!isOpen)
      {
        return;
      }
      if (ramWriter != null)
      {
        if (realtimeReader == null)
        {
          realtimeReader = DirectoryReader.open(ramWriter);
        } else
        {
          DirectoryReader newReader = DirectoryReader.openIfChanged(
              realtimeReader, ramWriter);
          if (newReader != null)
          {
            DirectoryReader tmpReader = realtimeReader;
            realtimeReader = newReader;
            tmpReader.close();
          }
        }
      }
    }
  }

  /**
   * Gets an index reader for search. This can be accessed by multiple threads
   * and cannot be blocking.
   *
   * @return an index reader containing in memory realtime index as well as
   *         persisted indexes. Null if the index is either closed or has no
   *         documents yet indexed.
   * @throws IOException
   */
  public IndexReader getIndexReader() throws IOException
  {
    // let's not build a reader if
    if (!isOpen)
    {
      return null;
    }
    List<DirectoryReader> readers = Lists
        .newArrayListWithCapacity(persistedReaders.size() + 1);
    readers.addAll(persistedReaders);
    DirectoryReader localReaderRef = realtimeReader;
    if (localReaderRef != null)
    {
      readers.add(localReaderRef);
    }
    return readers.isEmpty() ? null : new MultiReader(
        readers.toArray(new IndexReader[readers.size()]), false);
  }

  /**
   * Persists the segment to disk. This will be called by the same thread that
   * calls {@link #add(InputRow)} and {@link #close()}
   *
   * @throws IOException
   */
  public void persist() throws IOException
  {
    synchronized (refreshLock)
    {
      FSDirectory luceneDir = FSDirectory.open(indexDir(persistDir).toPath());
      try (IndexWriter persistWriter = buildPersistWriter(luceneDir))
      {
        ramWriter.commit();
        ramWriter.close();
        ramWriter = null;
        persistWriter.addIndexes(ramDir);
        persistWriter.commit();
        persistWriter.close();
        DirectoryReader reader = DirectoryReader.open(luceneDir);
        numRowsPersisted += reader.numDocs();
        persistedReaders.add(reader);
        DirectoryReader tmpReader = realtimeReader;
        realtimeReader = null;
        if (tmpReader != null)
        {
          tmpReader.close();
        }
      }
    }
  }

  /**
   * Closes this segment and a {@link #persist()} will be called if there are
   * pending writes.
   */
  @Override
  public void close() throws IOException
  {
    synchronized (refreshLock)
    {
      ensureOpen();
      isOpen = false;
      try
      {
        if (ramWriter.numDocs() > 0)
        {
          persist();
        }
        ramWriter.close();
        ramWriter = null;
        if (realtimeReader != null)
        {
          realtimeReader.close();
          realtimeReader = null;
        }
      } finally
      {
        for (DirectoryReader reader : persistedReaders)
        {
          try
          {
            reader.close();
          } catch (IOException ioe)
          {
            log.error(ioe.getMessage(), ioe);
          }
        }
        persistedReaders.clear();
      }
    }
  }

  @Override
  public String getIdentifier()
  {
    return segmentIdentifier.getIdentifierAsString();
  }

  @Override
  public Interval getDataInterval()
  {
    return segmentIdentifier.getInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StorageAdapter asStorageAdapter()
  {
    // TODO Auto-generated method stub
    return null;
  }

  public int numRows()
  {
    return (realtimeReader == null ? 0 : realtimeReader.numDocs())
        + numRowsPersisted;
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(LuceneDruidSegment.class))
    {
      return (T) this;
    } else
    {
      return null;
    }
  }

}
