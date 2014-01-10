package com.senseidb.abacus.api.codec.postings;

import com.senseidb.abacus.api.codec.common.DocIdSetCollector;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;

public class OpenBitsetDocIdSetCollector implements DocIdSetCollector {

  private OpenBitSet bs;
  public OpenBitsetDocIdSetCollector() {
    bs = null;
    
  }

  public void reset(int maxDoc) {
    if (bs == null) {
      bs = new OpenBitSet(maxDoc);
    }
    else {
      bs.clear(0, maxDoc);
    }
  }

  public void collect(int docid) {
    bs.set(docid);
  }

  public void flush(TermStats termStats, IndexOutput out) throws IOException {
    long[] bits = bs.getBits();
    out.writeVInt(bs.getNumWords());
    out.writeVInt(bits.length);
    for (long v : bits) {
      out.writeLong(v);
    }
  }

  public DocIdSet load(IndexInput in) throws IOException {
    int numWords = in.readVInt();
    int len = in.readVInt();
    long[] higherBitsArr = new long[len];
    for (int i = 0; i < len; ++i) {
      higherBitsArr[i] = in.readLong();
    }
    return new OpenBitSet(higherBitsArr, numWords);
  }

}
