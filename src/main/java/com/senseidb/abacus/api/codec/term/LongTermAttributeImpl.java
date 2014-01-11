package com.senseidb.abacus.api.codec.term;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;

public class LongTermAttributeImpl extends AttributeImpl implements TermToBytesRefAttribute {
    public final static int LONG_NUM_BYTES = 8;
  private final BytesRef bytes = new BytesRef(4 * LONG_NUM_BYTES);
  private long term;

  public static void copyIntToBytesRef(BytesRef bytes, long value) {
    ConversionUtils.fromLong(value, bytes.bytes, 0);
    bytes.offset = 0;
    bytes.length = LONG_NUM_BYTES;
  }

  @Override
  public int fillBytesRef() {
    copyIntToBytesRef(bytes, term);
    // important to return the correct hashcode here (from BytesRef)
    // otherwise EVERYTHING WILL GO TO HELL
    return bytes.hashCode();
  }

  @Override
  public BytesRef getBytesRef() {
    return bytes;
  }

  @Override
  public void clear() {
    this.term = 0;
  }

  @Override
  public void copyTo(AttributeImpl attribute) {
    LongTermAttributeImpl target = (LongTermAttributeImpl) attribute;
    target.term = term;
  }

  public void setTerm(long term) {
    this.term = term;
  }
}
