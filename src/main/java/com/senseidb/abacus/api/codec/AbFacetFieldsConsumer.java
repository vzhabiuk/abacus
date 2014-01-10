package com.senseidb.abacus.api.codec;

import org.apache.lucene.codecs.*;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AbFacetFieldsConsumer extends FieldsConsumer {

  private final SegmentWriteState state;
  private final PostingsWriterBase postingsWriter;
  
  static final String EXT = "bto";
  static final String CODEC = "BoboPrimitiveHashCodec";
  static final int VERSION = 0;
  private IndexOutput out;
  
  public AbFacetFieldsConsumer(SegmentWriteState state, PostingsWriterBase postingsWriter) throws IOException{
    this.state = state;
    this.postingsWriter = postingsWriter;
    
    final String termsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXT);
    out = state.directory.createOutput(termsFileName, state.context);
    
    CodecUtil.writeHeader(out, CODEC, VERSION);
    int numIndexedFields = 0;
    for (FieldInfo fi :  state.fieldInfos) {
      if (fi.isIndexed()) {
        numIndexedFields++;
      }
    }
    out.writeVInt(numIndexedFields);
    postingsWriter.start(out);
  }

  @Override
  public TermsConsumer addField(FieldInfo field) throws IOException {
    return new TermsWriter(field);
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeWhileHandlingException(out, postingsWriter);
  }
  
  private class TermsWriter extends TermsConsumer {
    private final int fieldId;
    private final FieldInfo field;
    List<BytesRef> termList = new ArrayList<BytesRef>();
    List<TermStats> termStats = new ArrayList<TermStats>();
    
    TermsWriter(FieldInfo field) {
      this.field = field;
      fieldId = field.number;
      postingsWriter.setField(field);
    }
    @Override
    public PostingsConsumer startTerm(BytesRef text) throws IOException {
      
      postingsWriter.startTerm();
      return postingsWriter;
    }

    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      // write stats info out to somewhere, this is per term
      termList.add(text.clone());
      termStats.add(stats);
      postingsWriter.finishTerm(stats);
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount)
        throws IOException {
      out.writeVInt(fieldId);
      BytesRef[] termArr = termList.toArray(new BytesRef[termList.size()]);
      
      // gen term buffer
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      for (BytesRef bref : termArr) {
        bout.write(bref.bytes, bref.offset, bref.length);
      }
      
      byte[] termBytes = bout.toByteArray();
      out.writeVInt(termBytes.length);
      out.writeBytes(termBytes, termBytes.length);
      
      out.writeVInt(termArr.length);
      for (int i=0; i<termArr.length; ++i) {
        BytesRef term = termArr[i];
        TermStats stats = termStats.get(i);
        out.writeVInt(term.length);
        out.writeVInt(stats.docFreq);
        if (field.getIndexOptions() != IndexOptions.DOCS_ONLY) {
          out.writeVLong(stats.totalTermFreq - stats.docFreq);
        }
      }
      if (field.getIndexOptions() != IndexOptions.DOCS_ONLY) {
        out.writeVLong(sumTotalTermFreq);
      }
      out.writeVLong(sumDocFreq);
      out.writeVInt(docCount);
      postingsWriter.flushTermsBlock(termArr.length, termArr.length); // all of it
      
    }

    @Override
    public Comparator<BytesRef> getComparator() throws IOException {
        return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    
  }
}
