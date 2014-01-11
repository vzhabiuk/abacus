package com.senseidb.abacus.api.codec.term;

import com.senseidb.abacus.api.codec.common.DocsEnumAdapter;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Comparator;

public class VaribleSizeTermEnum extends TermsEnum {
  private final Comparator<BytesRef> comparator;
  private final byte[] termBytes;
  private final int termCount;
  private int[] offsets;
  private final int[] frequencies;
  private final DocIdSet[] invertedIndexes;
  private int cursor = -1;

  public VaribleSizeTermEnum(byte[] termBytes, int termCount, int[] offsets, int[] frequencies,
                             DocIdSet[] invertedIndexes, final Comparator<BytesRef> comparator) {
    this.termBytes = termBytes;
    this.termCount = termCount;
    this.offsets = offsets;
    this.frequencies = frequencies;
    this.invertedIndexes = invertedIndexes;
    this.comparator = comparator;
  }

  @Override
  public BytesRef next() throws IOException {
    cursor ++;
    if (cursor >= termCount) return null;
    return term();
  }

  @Override
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }

  @Override
  public SeekStatus seekCeil(BytesRef text, boolean useCache)
    throws IOException {
    if (termCount == 0) {
      return SeekStatus.END;
    }
    int idx = binarySearch(termBytes, termCount, offsets, 0, termCount - 1, text, comparator);
    if (idx >= 0) {
      cursor = idx;
      return SeekStatus.FOUND;
    }
    else {
      idx = -(idx + 1);
      if (idx < termCount) {
        cursor = idx;
        return SeekStatus.NOT_FOUND;
      }
      else {
        cursor = -1;
        return SeekStatus.END;
      }
    }
  }
  public static int  binarySearch(byte[] a, int termCount, int[] offsets, int fromIndex, int toIndex,
                                  BytesRef value,
                                  Comparator<BytesRef> comparator) {
    BytesRef start = new BytesRef(a, offsets[fromIndex], getLength(fromIndex, offsets,a.length));
    for (int i = 0; i < termCount; i++) {
      System.out.println(new BytesRef(a, offsets[i], getLength(i, offsets,a.length)).utf8ToString());
    }
    BytesRef end = new BytesRef(a, offsets[toIndex], getLength(toIndex, offsets,a.length));
    BytesRef middle = new BytesRef(a, 0, 0);
    while (fromIndex <= toIndex){
      int mid = (fromIndex + toIndex)  >>> 1;
      middle.offset = offsets[mid];
      middle.length = getLength(mid,offsets,a.length);


      int comp = comparator.compare(middle, value);

      if (comp < 0) {
        //middle is less than value
        start.offset = offsets[mid + 1];
        start.length = getLength(mid + 1,offsets,a.length);
        fromIndex = mid + 1;
      } else if (comp > 0)  {
        end.offset = offsets[mid - 1];
        end.length = getLength(mid - 1,offsets,a.length);
        toIndex = mid - 1;
      } else {
        return mid;
      }
    }
    return -(fromIndex + 1);  // key not found.
  }

  public int getLength(int ord) {
    if (ord < offsets.length - 1) {
      return offsets[ord + 1] - offsets[ord];
    } else if (ord == offsets.length - 1) {
      return termBytes.length - offsets[ord];
    } else {
      throw new UnsupportedOperationException("ord os out of bounds" + ord);
    }
  }
  public static int getLength(int ord, int[] offsets, int byteLength) {
    if (ord < offsets.length - 1) {
      return offsets[ord + 1] - offsets[ord];
    } else if (ord == offsets.length - 1) {
      return byteLength - offsets[ord];
    } else {
      throw new UnsupportedOperationException("ord os out of bounds" + ord);
    }
  }
  @Override
  public void seekExact(long ord) throws IOException {
    cursor = (int)ord;
  }

  @Override
  public BytesRef term() throws IOException {
    return new BytesRef(termBytes, offsets[cursor], getLength(cursor));
  }

  @Override
  public long ord() throws IOException {
    return cursor;
  }

  @Override
  public int docFreq() throws IOException {
    return frequencies[cursor];
  }

  @Override
  public long totalTermFreq() throws IOException {
    return -1;
  }

  @Override
  public TermState termState() throws IOException {
    return new TermState() {
      @Override
      public void copyFrom(TermState other) {
        //nothing
      }
    };
  }

  @Override
  public DocsEnum docs(Bits liveDocs, DocsEnum reuse, int flags)
    throws IOException {
    return new DocsEnumAdapter(invertedIndexes[cursor], frequencies[cursor]);
  }

  @Override
  public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
                                               DocsAndPositionsEnum reuse, int flags) throws IOException {
    return null;
  }
}