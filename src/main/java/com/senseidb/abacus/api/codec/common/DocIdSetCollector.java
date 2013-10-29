package com.senseidb.abacus.api.codec.common;

import java.io.IOException;

import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public interface DocIdSetCollector {
  void reset(int maxDoc);
  void collect(int docid);
  void flush(TermStats termStats, IndexOutput out) throws IOException;
  
  DocIdSet load(IndexInput in) throws IOException;
}
