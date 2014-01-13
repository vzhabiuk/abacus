package com.senseidb.abacus.api.codec;

import com.senseidb.abacus.api.codec.common.DocIdSetCollector;
import com.senseidb.abacus.api.codec.postings.DefaultDocIdSetCollector;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

public class AbFacetPostingsWriter extends PostingsWriterBase {

  final static String DOC_CODEC = "BoboFacetPostingsWriterDoc";
  final static String TERMS_CODEC = "BoboFacetPostingsWriterTerms";

  public final static int BLOCK_SIZE = 128;

  final static int VERSION_START = 0;
  final static int VERSION_CURRENT = VERSION_START;
  final IndexOutput docOut;
  private int currentDoc = -1;

  private final int maxDoc;
  private final DocIdSetCollector docidCollector;

  public AbFacetPostingsWriter(SegmentWriteState state) throws IOException{
    this.docidCollector = new DefaultDocIdSetCollector();
    maxDoc = state.segmentInfo.getDocCount();

    docOut = state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix,
      AbacusFacetPostingsFormat.DOC_EXTENSION),
      state.context);
    boolean success = false;
    try {
      CodecUtil.writeHeader(docOut, DOC_CODEC, VERSION_CURRENT);
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut);
      }
    }
  }

  @Override
  public void start(IndexOutput termsOut) throws IOException {
    CodecUtil.writeHeader(termsOut, TERMS_CODEC, VERSION_CURRENT);
    termsOut.writeVInt(BLOCK_SIZE);
  }


  @Override
  public void startTerm() throws IOException {
    docidCollector.reset(maxDoc);
  }

  @Override
  public void finishTerm(TermStats stats) throws IOException {
    docidCollector.flush(stats, docOut);
    docOut.flush();
  }

  @Override
  public void flushTermsBlock(int start, int count) throws IOException {
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    assert IndexOptions.DOCS_ONLY == fieldInfo.getIndexOptions();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(docOut);
  }

  @Override
  public void startDoc(int docID, int freq) throws IOException {
    currentDoc = docID;
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset,
                          int endOffset) throws IOException {
    // we don't handle positions for facet fields
  }

  @Override
  public void finishDoc() throws IOException {
    docidCollector.collect(currentDoc);
  }
}
