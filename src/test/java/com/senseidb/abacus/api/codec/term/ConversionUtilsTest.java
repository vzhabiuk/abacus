package com.senseidb.abacus.api.codec.term;

import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class ConversionUtilsTest {
    @Test
    public void testIntConversion() throws Exception {
        int[] arr = new int[] {Integer.MIN_VALUE, (int)Short.MIN_VALUE, -1, 0 , 1, (int)Short.MAX_VALUE, Integer.MAX_VALUE};
        reverse(arr);
        BytesRef[] bytes = new BytesRef[arr.length];
        for (int i = 0; i < arr.length; i++) {
            bytes[i] = new BytesRef(ConversionUtils.fromInt(arr[i]));

        }
        Arrays.sort(bytes, BytesRef.getUTF8SortedAsUnicodeComparator());
        int[] arr2 = new int[bytes.length];
        for (int i = 0; i < arr2.length; i++) {
            arr2[i] = ConversionUtils.toInt(bytes[i].bytes, 0);
        }
        reverse(arr);
        assertEquals(Arrays.toString(arr), Arrays.toString(arr2));
    }
  @Test
  public void testLongConversion() throws Exception {
    long[] arr = new long[] {Long.MIN_VALUE, Short.MIN_VALUE- 10, -2, 0 , 1, Short.MAX_VALUE, Long.MAX_VALUE};
    reverse(arr);
    BytesRef[] bytes = new BytesRef[arr.length];
    for (int i = 0; i < arr.length; i++) {
      bytes[i] = new BytesRef(ConversionUtils.fromLong(arr[i]));
      //System.out.println(Arrays.toString(ConversionUtils.fromLong(arr[i])));
    }
    Arrays.sort(bytes, BytesRef.getUTF8SortedAsUnicodeComparator());
    long[] arr2 = new long[bytes.length];
    for (int i = 0; i < arr2.length; i++) {
      arr2[i] = ConversionUtils.toLong(bytes[i].bytes, 0);
    }
    reverse(arr);
    assertEquals(Arrays.toString(arr), Arrays.toString(arr2));
  }
  public static void reverse(int[] array) {
        for (int i = 0; i < array.length / 2; i++) {
            int temp = array[i];
            array[i] = array[array.length - 1 - i];
            array[array.length - 1 - i] = temp;
        }
    }
  public static void reverse(long[] array) {
    for (int i = 0; i < array.length / 2; i++) {
      long temp = array[i];
      array[i] = array[array.length - 1 - i];
      array[array.length - 1 - i] = temp;
    }
  }
}
