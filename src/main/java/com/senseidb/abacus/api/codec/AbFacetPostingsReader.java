package com.senseidb.abacus.api.codec;

import com.senseidb.abacus.api.codec.common.DocIdSetCollector;
import com.senseidb.abacus.api.codec.postings.DefaultDocIdSetCollector;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

import java.io.Closeable;
import java.io.IOException;

public class AbFacetPostingsReader implements Closeable{

  private final IndexInput docIn;
  private final DocIdSetCollector docIdSetCollector;

  public AbFacetPostingsReader(SegmentReadState state) throws IOException{
    boolean success = false;
    this.docIdSetCollector = new DefaultDocIdSetCollector();
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
