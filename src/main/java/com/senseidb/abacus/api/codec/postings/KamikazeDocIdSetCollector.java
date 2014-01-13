package com.senseidb.abacus.api.codec.postings;

import com.senseidb.abacus.api.codec.common.DocIdSetCollector;
import com.senseidb.abacus.api.codec.utils.PForDeltaDocIdSet;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

public class KamikazeDocIdSetCollector implements DocIdSetCollector {

  private PForDeltaDocIdSet pForDeltaDocIdSet;
  public KamikazeDocIdSetCollector() {

  }

  public void reset(int maxDoc) {
    pForDeltaDocIdSet = new PForDeltaDocIdSet();
  }

  public void collect(int docid) {
    try {
      pForDeltaDocIdSet.addDoc(docid);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void flush(TermStats termStats, IndexOutput out) throws IOException {
    byte[] arr = PForDeltaDocIdSet.serialize(pForDeltaDocIdSet);
    out.writeVInt(arr.length);
    out.writeBytes(arr, arr.length);
  }

  public DocIdSet load(IndexInput in) throws IOException {
    int length = in.readVInt();
    byte[] arr = new byte[length];
    in.readBytes(arr, 0, length);
    return PForDeltaDocIdSet.deserialize(arr, 0);
  }

}
