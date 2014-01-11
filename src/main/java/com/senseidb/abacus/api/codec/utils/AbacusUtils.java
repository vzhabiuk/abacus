package com.senseidb.abacus.api.codec.utils;

import com.senseidb.abacus.api.codec.common.SingleTokenStream;
import com.senseidb.abacus.api.codec.term.IntTermAttributeImpl;
import com.senseidb.abacus.api.codec.term.LongTermAttributeImpl;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;

public class AbacusUtils {
  public static Field field(String fieldName, int value, FieldType type) {
    IntTermAttributeImpl attrImpl = new IntTermAttributeImpl();
    attrImpl.setTerm(value);
    SingleTokenStream tokStream = new SingleTokenStream();
    tokStream.addAttributeImpl(attrImpl);
    return new Field(fieldName, tokStream, type);
  }
  public static Field field(String fieldName, long value, FieldType type) {
    LongTermAttributeImpl attrImpl = new LongTermAttributeImpl();
    attrImpl.setTerm(value);
    SingleTokenStream tokStream = new SingleTokenStream();
    tokStream.addAttributeImpl(attrImpl);
    return new Field(fieldName, tokStream, type);
  }
}
