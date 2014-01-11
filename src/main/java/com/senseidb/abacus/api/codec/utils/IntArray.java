package com.senseidb.abacus.api.codec.utils;

import com.kamikaze.docidset.utils.Conversion;
import com.kamikaze.docidset.utils.PrimitiveArray;

import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public class IntArray extends PrimitiveArray<Integer> implements Serializable {

  private static final long serialVersionUID = 1L;

  public IntArray(int len) {
    super(len);
  }

  public IntArray() {
    super();
  }

  public void add(int val) {
    ensureCapacity(_count + 1);
    int[] array = (int[]) _array;
    array[_count] = val;
    _count++;
  }


  public void set(int index, int val) {
    ensureCapacity(index);
    int[] array = (int[]) _array;
    array[index] = val;
    _count = Math.max(_count, index + 1);
  }

  public int get(int index) {
    int[] array = (int[]) _array;
    return array[index];
  }

  public boolean contains(int elem) {
    int size = this.size();
    for (int i = 0; i < size; ++i) {
      if (get(i) == elem)
        return true;
    }
    return false;
  }

  @Override
  protected Object buildArray(int len) {
    return new int[len];
  }

  private static int binarySearch(int[] a, int fromIndex, int toIndex,int key) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a[mid];

      if (midVal < key)
        low = mid + 1;
      else if (midVal > key)
        high = mid - 1;
      else
        return mid; // key found
    }
    return -(low + 1);  // key not found.
  }

  public static int getSerialIntNum(IntArray instance)
  {
    int num = 3 + instance._count; // _len, _count, _growth
    return num;
  }

  public static int convertToBytes(IntArray instance, byte[] out, int offset)
  {
    int numInt = 0;
    Conversion.intToByteArray(instance._len, out, offset);
    offset += Conversion.BYTES_PER_INT;
    numInt++;

    Conversion.intToByteArray(instance._count, out, offset);
    offset += Conversion.BYTES_PER_INT;
    numInt++;

    Conversion.intToByteArray(instance._growth, out, offset);
    offset += Conversion.BYTES_PER_INT;
    numInt++;

    for(int i=0; i<instance.size(); i++)
    {
      int data = instance.get(i);
      Conversion.intToByteArray(data, out, offset);
      offset += Conversion.BYTES_PER_INT;
    }
    numInt += instance.size();
    return numInt;
  }

  public static IntArray newInstanceFromBytes(byte[] inData, int offset) throws IOException
  {
    int len = Conversion.byteArrayToInt(inData, offset);
    offset += Conversion.BYTES_PER_INT;

    IntArray instance = len > 0 ? new IntArray(len): new IntArray();

    int count = Conversion.byteArrayToInt(inData, offset);
    offset += Conversion.BYTES_PER_INT;

    int growth =  Conversion.byteArrayToInt(inData, offset);
    offset += Conversion.BYTES_PER_INT;

    for(int i=0; i<count; i++)
    {
      int data = Conversion.byteArrayToInt(inData, offset);
      offset += Conversion.BYTES_PER_INT;
      instance.add(data);
    }

    instance._growth = growth;
    if(instance._count != count)
      throw new IOException("cannot build IntArray from byte[]");

    return instance;
  }
}
