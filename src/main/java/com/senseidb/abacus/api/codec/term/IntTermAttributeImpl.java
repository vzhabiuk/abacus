package com.senseidb.abacus.api.codec.term;

import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;

public class IntTermAttributeImpl extends AttributeImpl implements TermToBytesRefAttribute {
    public final static int INTEGER_NUM_BYTES = 4;
    private final BytesRef bytes = new BytesRef(4 * INTEGER_NUM_BYTES);
    private int term;

    public static void copyIntToBytesRef(BytesRef bytes, int value) {
        ConversionUtils.fromInt(value, bytes.bytes, 0);
        bytes.offset = 0;
        bytes.length = INTEGER_NUM_BYTES;
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
        IntTermAttributeImpl target = (IntTermAttributeImpl) attribute;
        target.term = term;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
