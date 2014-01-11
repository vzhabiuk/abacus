package com.senseidb.abacus.api.codec.term;

import com.senseidb.abacus.api.codec.AbFacetFieldsProducer;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;

public class BytesTerms extends AbstractTerms {
  private final byte[] terms;
  private final int termsSize;

  public BytesTerms(byte[] terms, int termsSize, int[] frequencies,
                    AbFacetFieldsProducer.FieldMeta fieldMeta, DocIdSet[] invertedIndexes,
                    Comparator<BytesRef> comparator) {
    super(frequencies, comparator, invertedIndexes, fieldMeta);
    this.terms = terms;
    this.termsSize = termsSize;
  }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
        return new BytesTermEnum(terms, termsSize, frequencies, invertedIndexes, comparator);
    }

  @Override
    public long size() throws IOException {
        return terms.length / termsSize;
    }

}
