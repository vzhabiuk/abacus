package com.senseidb.abacus.api.codec.common;

import org.apache.lucene.analysis.TokenStream;

/**
 * Helper TokenStream implementation that just returns a single
 * token to Lucene's IndexWriter.
 */
public class SingleTokenStream extends TokenStream {
    boolean exhausted = false;

    @Override
    public final boolean incrementToken() {
        if (exhausted) {
            return false;
        } else {
            exhausted = true;
            return true;
        }
    }

    @Override
    public void reset() {
        exhausted = false;
    }
}