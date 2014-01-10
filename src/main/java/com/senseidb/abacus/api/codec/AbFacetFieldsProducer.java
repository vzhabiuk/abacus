package com.senseidb.abacus.api.codec;

import com.senseidb.abacus.api.codec.term.BytesTerms;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class AbFacetFieldsProducer extends FieldsProducer {  
  private IndexInput in;
  private final AbFacetPostingsReader postingsReader;
  private Map<String, int[]> frequenciesMap;
  private Map<String, DocIdSet[]> invertedIndexes;
  private Map<String, byte[]> termsMap;
  private Map<String, FieldMeta> fieldMetaMap;
  private Map<String, Integer> termsLength;
  public static class FieldMeta {
      public FieldInfo field;
      public int docCount;
      public int termCount;
      public long sumTotalTermFreq = -1;
      public long sumDocFreq;
  }

  public static class TermMetaWithState {
      public int df;
      public long totalTf;
      public BlockTermState termState;
  }
  
  public AbFacetFieldsProducer(SegmentReadState state, AbFacetPostingsReader postingsReader) throws IOException{
    this.postingsReader = postingsReader;

    frequenciesMap = new HashMap<String, int[]>();
    fieldMetaMap = new HashMap<String, FieldMeta>();
    invertedIndexes = new HashMap<String, DocIdSet[]>();
    termsMap = new HashMap<String, byte[]>();
    termsLength = new HashMap<String, Integer>();
    in = state.directory.openInput(IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, AbFacetFieldsConsumer.EXT),
        state.context);
    int version = CodecUtil.checkHeader(in, AbFacetFieldsConsumer.CODEC,0,0);
    if (version != AbFacetFieldsConsumer.VERSION) {
      throw new IOException("invalid version: " + version);
    }

    int fieldCount = in.readVInt();
    postingsReader.init(in);
    for (int i = 0; i < fieldCount; ++i) {
      FieldMeta fieldMeta = new FieldMeta();
      
      int fieldId = in.readVInt();
      fieldMeta.field = state.fieldInfos.fieldInfo(fieldId);
      
      fieldMetaMap.put(fieldMeta.field.name, fieldMeta);
      
      int bytesLen = in.readVInt();
      byte[] termBytes = new byte[bytesLen];
      in.readBytes(termBytes, 0, bytesLen);
          
      fieldMeta.termCount = in.readVInt();
      
      //BytesRef[] terms = new BytesRef[fieldMeta.termCount];
      int[] frequencies = new int[fieldMeta.termCount];
      DocIdSet[] inverted = new DocIdSet[fieldMeta.termCount];
      int[] lengths = new int[fieldMeta.termCount];
      for (int k = 0; k <fieldMeta.termCount; ++k) {
        lengths[k] = in.readVInt();
        frequencies[k] = in.readVInt();
        inverted[k] = postingsReader.loadNextPostingsList();
      }
      for (int j = 1; j < lengths.length; j++) {
        if (lengths[0] != lengths[j]) {
          throw new UnsupportedOperationException("All term lengths should be equal");
        }
      }
      termsLength.put(fieldMeta.field.name, lengths[0]);
      invertedIndexes.put(fieldMeta.field.name, inverted);
      frequenciesMap.put(fieldMeta.field.name, frequencies);
      fieldMeta.sumDocFreq = in.readVLong();
      fieldMeta.docCount = in.readVInt();
      termsMap.put(fieldMeta.field.name, termBytes);
    }
  }
  
  @Override
  public void close() throws IOException {
    IOUtils.closeWhileHandlingException(in, postingsReader);
  }

  @Override
  public Iterator<String> iterator() {
    return fieldMetaMap.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException {
    if (termsMap.containsKey(field)) {
      return new BytesTerms(termsMap.get(field), termsLength.get(field),frequenciesMap.get(field),fieldMetaMap.get(field),
         invertedIndexes.get(field), BytesRef.getUTF8SortedAsUnicodeComparator());
    } else {
      return null;
    }
  }

  @Override
  public int size() {
    return fieldMetaMap.size();
  }

}
