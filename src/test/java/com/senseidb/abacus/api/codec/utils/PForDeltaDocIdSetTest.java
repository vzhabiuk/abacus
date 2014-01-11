package com.senseidb.abacus.api.codec.utils;

import org.junit.Test;

public class PForDeltaDocIdSetTest {
  @Test
  public void testDeserialize() throws Exception {
    PForDeltaDocIdSet pForDeltaDocIdSet = new PForDeltaDocIdSet();
    for (int i : new int[] {1,5,13,22,100}) {
      pForDeltaDocIdSet.addDoc(i);
    }
    pForDeltaDocIdSet.optimize();
    byte[] arr = PForDeltaDocIdSet.serialize(pForDeltaDocIdSet);
    System.out.println("--------------------------------------------------");
    PForDeltaDocIdSet deser = PForDeltaDocIdSet.deserialize(arr, 0);
    PForDeltaDocIdSet.PForDeltaDocIdIterator iterator = deser.iterator();
    int i = 0;
    while (i++ < 10) {
      System.out.println("it = " + iterator.nextDoc());
    }
  }

  @Test
  public void testSerialize() throws Exception {

  }
}
