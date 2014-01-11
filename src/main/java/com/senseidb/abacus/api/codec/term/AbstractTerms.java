package com.senseidb.abacus.api.codec.term;

import com.senseidb.abacus.api.codec.AbFacetFieldsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;

public abstract class AbstractTerms extends Terms {
  protected final AbFacetFieldsProducer.FieldMeta fieldMeta;
  protected final Comparator<BytesRef> comparator;
  protected final int[] frequencies;
  protected DocIdSet[] invertedIndexes;

  public AbstractTerms(int[] frequencies, Comparator<BytesRef> comparator, DocIdSet[] invertedIndexes, AbFacetFieldsProducer.FieldMeta fieldMeta) {
    this.frequencies = frequencies;
    this.comparator = comparator;
    this.invertedIndexes = invertedIndexes;
    this.fieldMeta = fieldMeta;
  }

  @Override
  public Comparator<BytesRef> getComparator() {
      return comparator;
  }

  @Override
  public long getSumTotalTermFreq() throws IOException {
      return fieldMeta.sumTotalTermFreq;
  }

  @Override
  public long getSumDocFreq() throws IOException {
      return fieldMeta.sumDocFreq;
  }

  @Override
  public int getDocCount() throws IOException {
      return fieldMeta.docCount;
  }

  @Override
  public boolean hasOffsets() {
      return fieldMeta.field.getIndexOptions().compareTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  public boolean hasPositions() {
      return fieldMeta.field.getIndexOptions().compareTo(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
  }

  @Override
  public boolean hasPayloads() {
      return fieldMeta.field.hasPayloads();
  }
}
