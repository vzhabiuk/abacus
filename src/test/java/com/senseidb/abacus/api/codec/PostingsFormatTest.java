package com.senseidb.abacus.api.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.util._TestUtil;

public class PostingsFormatTest extends BasePostingsFormatTestCase{
  private final Codec codec = _TestUtil.alwaysPostingsFormat(new AbacusFacetPostingsFormat());
  
  @Override
  protected Codec getCodec() {
    return codec;
  }
    
}
