package com.senseidb.abacus.api.codec.common;

import java.io.IOException;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

public class DelegatingDocValuesFormat extends DocValuesFormat {
  
  private final DocValuesFormat delegate;
  public DelegatingDocValuesFormat(String name, DocValuesFormat delegate) {
    super(name);
    this.delegate = delegate;
  }

  @Override
  public DocValuesConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    return new DelegatingDocValuesConsumer(state, delegate.fieldsConsumer(state));
  }

  @Override
  public DocValuesProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    return new DelegatingDocValuesProducer(state, delegate.fieldsProducer(state));
  }

}
