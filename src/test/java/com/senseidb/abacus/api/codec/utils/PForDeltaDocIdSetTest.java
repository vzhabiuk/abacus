package com.senseidb.abacus.api.codec.utils;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class PForDeltaDocIdSetTest {
  @Test
  public void tesSerializetDeserialize() throws Exception {
    PForDeltaDocIdSet pForDeltaDocIdSet = new PForDeltaDocIdSet();
    int[] docIds = {1, 5, 13, 22, 100};
    for (int i : docIds) {
      pForDeltaDocIdSet.addDoc(i);
    }
    pForDeltaDocIdSet.optimize();
    byte[] arr = PForDeltaDocIdSet.serialize(pForDeltaDocIdSet);
    PForDeltaDocIdSet deserialized = PForDeltaDocIdSet.deserialize(arr, 0);
    PForDeltaDocIdSet.PForDeltaDocIdIterator iterator = deserialized.iterator();
    int i = 0;
    while (i < docIds.length) {
      Assert.assertEquals(docIds[i], iterator.nextDoc());
      i++;
    }
  }

  @Test
  public void testPerf() throws Exception {
    Random rand = new Random();
    int[] arr = new int[100011];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = Math.abs(rand.nextInt(arr.length * 500));
    }
    Arrays.sort(arr);
    long time = System.currentTimeMillis();
    PForDeltaDocIdSet pForDeltaDocIdSet = new PForDeltaDocIdSet();
    pForDeltaDocIdSet.addDoc(arr[0]);
    for (int i = 1; i < arr.length; i++) {
       if (arr[i - 1] != arr[i]) {
         pForDeltaDocIdSet.addDoc(arr[i]);
       }
    }
    System.out.println("Insertion - " + (System.currentTimeMillis() - time));
    pForDeltaDocIdSet.optimize();
    time = System.currentTimeMillis();
    byte[] ser = PForDeltaDocIdSet.serialize(pForDeltaDocIdSet);
    System.out.println("serialization - " + (System.currentTimeMillis() - time));
    System.out.println("Serialized length = " + ser.length);
    time = System.currentTimeMillis();
    PForDeltaDocIdSet.PForDeltaDocIdIterator iterator = pForDeltaDocIdSet.iterator();
    for (int i = 1; i < arr.length; i+=40) {
      Assert.assertEquals(arr[i], iterator.advance(arr[i]));
    }
    System.out.println("iteration - " + (System.currentTimeMillis() - time));
  }
}
