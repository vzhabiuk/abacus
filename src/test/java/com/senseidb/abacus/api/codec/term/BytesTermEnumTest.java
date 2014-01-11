package com.senseidb.abacus.api.codec.term;

import org.apache.lucene.util.BytesRef;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class BytesTermEnumTest {
  @Test
  public void testBinarySearch() throws Exception {
    int numElems = 10;
    int[] intArr = new int[numElems];
    byte[] arr = new byte[numElems * 4];
    for (int i = 0; i < numElems; i++) {
      ConversionUtils.fromInt(i * 2, arr, i * 4);
      intArr[i] = i * 2;
    }
    for (int i = 0; i < numElems * 2; i++) {
      int position = findElem(numElems, arr, i);
      int positionToCompare = Arrays.binarySearch(intArr, i);
      if (i % 2 == 0) {
        Assert.assertEquals(position, i / 2);
        Assert.assertEquals(position, positionToCompare);
      } else {
        Assert.assertEquals(position, positionToCompare);
      }

    }


  }

  private int findElem(int numElems, byte[] arr, int value) {
    return BytesTermEnum.binarySearch(arr, 4, new BytesRef(arr, 0, 4), new BytesRef(arr, (numElems - 1) * 4, 4),
      new BytesRef(ConversionUtils.fromInt(value)), BytesRef.getUTF8SortedAsUnicodeComparator());
  }
}
