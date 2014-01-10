package com.senseidb.abacus.api.codec;

import com.senseidb.abacus.api.codec.common.DocIdSetCollector;
import com.senseidb.abacus.api.codec.postings.OpenBitsetDocIdSetCollector;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class AbFacetPostingsReader implements Closeable{

  private final IndexInput docIn;
  private final DocIdSetCollector docIdSetCollector;

  public AbFacetPostingsReader(SegmentReadState state) throws IOException{
    boolean success = false;
    this.docIdSetCollector = new OpenBitsetDocIdSetCollector();
    IndexInput in = null;
    try {
      in = state.directory.openInput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, AbacusFacetPostingsFormat.DOC_EXTENSION),
          state.context);
      CodecUtil.checkHeader(in,
          AbFacetPostingsWriter.DOC_CODEC,
          AbFacetPostingsWriter.VERSION_CURRENT,
          AbFacetPostingsWriter.VERSION_CURRENT);

      this.docIn = in;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }
  public void init(IndexInput termsIn) throws IOException {
    // Make sure we are talking to the matching postings writer
    CodecUtil.checkHeader(termsIn,
        AbFacetPostingsWriter.TERMS_CODEC,
        AbFacetPostingsWriter.VERSION_CURRENT,
        AbFacetPostingsWriter.VERSION_CURRENT);
    final int indexBlockSize = termsIn.readVInt();
    if (indexBlockSize != AbFacetPostingsWriter.BLOCK_SIZE) {
      throw new IllegalStateException("index-time BLOCK_SIZE (" + indexBlockSize + ") != read-time BLOCK_SIZE (" + AbFacetPostingsWriter.BLOCK_SIZE + ")");
    }
  }

  public void close() throws IOException {
    IOUtils.close(docIn);
  }


  public DocIdSet loadNextPostingsList() throws IOException {
    return docIdSetCollector.load(docIn);
  }
}
