package com.senseidb.abacus.api.codec.common;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

public class DocsEnumAdapter extends DocsEnum{
  private final DocIdSetIterator docIdSetIterator;
  private DocIdSet set;
  private int df;

  public DocsEnumAdapter(DocIdSet set, int df) {
    this.set = set;
    this.df = df;
    try {
       docIdSetIterator = set.iterator();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int nextDoc() throws IOException {
    return docIdSetIterator.nextDoc();
  }

  @Override
  public int docID() {
    return docIdSetIterator.docID();
  }

  @Override
  public long cost() {
    return df;
  }

  @Override
  public int advance(int target) throws IOException {
    int doc = docIdSetIterator.advance(target);
    if (doc == DocIdSetIterator.NO_MORE_DOCS) return doc;
    return nextDoc();
  }

  @Override
  public int freq() throws IOException {
    return 1;
  }
}
