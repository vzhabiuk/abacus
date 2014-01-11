package com.senseidb.abacus.api.codec.term;

public class ConversionUtils {
    public static byte[] fromInt(int value) {
       byte[] arr = new byte[4];
        fromInt(value, arr, 0);
       return arr;
    }
    public static void fromInt(int value, byte[] arr, int offset) {
        value ^= 1 << 31;//toggling highest bit
        arr[offset] = (byte)(value >>> 24);
        arr[offset + 1] = (byte)(value >>> 16);
        arr[offset + 2] = (byte)(value >>> 8);
        arr[offset + 3] = (byte)value;
    }
    public static int toInt(byte[] bytes, int offset) {
        int ret = 0;
        for (int i = offset; i < offset + 4 ; i++) {
            ret <<= 8;
            ret |= (int)bytes[i] & 0xFF;
        }
        ret ^= 1 << 31;//toggling highest bit;
        return ret;
    }
  public static byte[] fromLong(long value) {
    byte[] arr = new byte[8];
    fromLong(value, arr, 0);
    return arr;
  }
  public static void fromLong(long value, byte[] arr, int offset) {
    value ^= 1L << 63;//toggling highest bit
    arr[offset] = (byte)(value >>> 56);
    arr[offset + 1] = (byte)(value >>> 48);
    arr[offset + 2] = (byte)(value >>> 40);
    arr[offset + 3] = (byte)(value >>> 32);
    arr[offset + 4] = (byte)(value >>> 24);
    arr[offset + 5] = (byte)(value >>> 16);
    arr[offset + 6] = (byte)(value >>> 8);
    arr[offset + 7] = (byte)value;
  }
  public static long toLong(byte[] bytes, int offset) {
    long ret = 0;
    for (int i = offset; i < offset + 8 ; i++) {
      ret <<= 8;
      ret |= (long)bytes[i] & 0xFF;
    }
    ret ^= 1L << 63;//toggling highest bit;
    return ret;
  }
//    public static int toInt(byte[] bytes) {
//        int value = bytes[0] << 24 | bytes[1] << 16 | bytes[2] << 8 | bytes[3];
//        //if (value < 0) value ^= 0x7fffffff;
//        return value;
//    }

}
