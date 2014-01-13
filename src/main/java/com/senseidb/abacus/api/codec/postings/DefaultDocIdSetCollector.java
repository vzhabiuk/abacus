package com.senseidb.abacus.api.codec.postings;

import com.senseidb.abacus.api.codec.common.DocIdSetCollector;
import com.senseidb.abacus.api.codec.utils.PForDeltaDocIdSet;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.OpenBitSet;

import java.io.IOException;

public class DefaultDocIdSetCollector implements DocIdSetCollector {

  public static final int BITSET_ID = 1;
  public static final int KAMIKAZE_ID = 2;
  private OpenBitSet bs;
  private int maxDoc;

  public DefaultDocIdSetCollector() {
    bs = null;
    
  }

  public void reset(int maxDoc) {
    this.maxDoc = maxDoc;
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
    if (maxDoc / termStats.docFreq <= 8) {
      out.writeVInt(BITSET_ID);
      long[] bits = bs.getBits();
      out.writeVInt(bs.getNumWords());
      out.writeVInt(bits.length);
      for (long v : bits) {
        out.writeLong(v);
      }
    } else {
      PForDeltaDocIdSet pForDeltaDocIdSet = new PForDeltaDocIdSet();
      int i = 0;
      while((i = bs.nextSetBit(i)) >= 0) {
        pForDeltaDocIdSet.addDoc(i);
        i++;
      }
      pForDeltaDocIdSet.optimize();
      byte[] arr = PForDeltaDocIdSet.serialize(pForDeltaDocIdSet);
      out.writeVInt(KAMIKAZE_ID);
      out.writeVInt(arr.length);
      out.writeBytes(arr, arr.length);
    }


  }

  public DocIdSet load(IndexInput in) throws IOException {
    int algo =  in.readVInt();
    if (algo == BITSET_ID) {
      int numWords = in.readVInt();
      int len = in.readVInt();
      long[] higherBitsArr = new long[len];
      for (int i = 0; i < len; ++i) {
        higherBitsArr[i] = in.readLong();
      }
      return new OpenBitSet(higherBitsArr, numWords);
    } else if (algo == KAMIKAZE_ID) {
      int length = in.readVInt();
      byte[] arr = new byte[length];
      in.readBytes(arr, 0, length);
      return PForDeltaDocIdSet.deserialize(arr, 0);
    } else {
      throw new UnsupportedOperationException("algo is unsupported - " + algo);
    }
  }

}
