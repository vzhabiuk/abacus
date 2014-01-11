package com.senseidb.abacus.api.codec.term;

import com.senseidb.abacus.api.codec.AbFacetFieldsProducer;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;

public class VariableSizeTerms extends AbstractTerms {
  private final byte[] terms;
  private int termCount;
  private int[] offsets;

  public VariableSizeTerms(byte[] terms, int termCount, int[] offsets, int[] frequencies,
                    AbFacetFieldsProducer.FieldMeta fieldMeta, DocIdSet[] invertedIndexes,
                    Comparator<BytesRef> comparator) {
    super(frequencies, comparator, invertedIndexes, fieldMeta);
    this.terms = terms;
    this.termCount = termCount;
    this.offsets = offsets;
  }

  @Override
  public TermsEnum iterator(TermsEnum reuse) throws IOException {
    return new VaribleSizeTermEnum(terms, termCount, offsets, frequencies, invertedIndexes, comparator);
  }

  @Override
  public long size() throws IOException {
    return terms.length / termCount;
  }

}