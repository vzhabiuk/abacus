package com.senseidb.abacus.api.codec;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

public class AbacusDocValuesProducer extends DocValuesProducer {

  private final SegmentReadState state;
  private final DocValuesProducer delegate;
  
  public AbacusDocValuesProducer(SegmentReadState state, DocValuesProducer delegate) {
    this.state = state;
    this.delegate = delegate;
  }
  
  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    return delegate.getNumeric(field);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    return delegate.getBinary(field);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    return delegate.getSorted(field);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    return delegate.getSortedSet(field);
  }

}
