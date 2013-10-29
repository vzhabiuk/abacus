package com.senseidb.abacus.api.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;

import com.senseidb.abacus.api.codec.common.DelegatingDocValuesFormat;

public class AbacusCodec extends FilterCodec {

	private static final String CODEC_NAME = "AbacusCodec";
	private static final Codec luceneCodec = new Lucene42Codec();
	
	private static final AbacusFacetPostingsFormat postingsFormat = new AbacusFacetPostingsFormat();
	public AbacusCodec() {
		super(CODEC_NAME, luceneCodec);
	}

  @Override
  public DocValuesFormat docValuesFormat() {
    return new DelegatingDocValuesFormat("AbacusValuesFormat", luceneCodec.docValuesFormat());
  }

  @Override
  public PostingsFormat postingsFormat() {
    return postingsFormat;
  }
	
  
}
