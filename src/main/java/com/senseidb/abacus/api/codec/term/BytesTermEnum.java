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

public class BytesTermEnum extends TermsEnum {
  private final Comparator<BytesRef> comparator;
  private final byte[] termBytes;
  private final int termSize;
  private final int[] frequencies;
  private final DocIdSet[] invertedIndexes;
  private final int termsCount;
  private int cursor = -1;

    public BytesTermEnum( byte[] termBytes, int termSize, int[] frequencies,
                         DocIdSet[] invertedIndexes, final Comparator<BytesRef> comparator) {
      this.termBytes = termBytes;
      this.termSize = termSize;
      this.frequencies = frequencies;
      this.invertedIndexes = invertedIndexes;
      termsCount = termBytes.length / termSize;
        this.comparator = comparator;
    }

    @Override
    public BytesRef next() throws IOException {
        cursor ++;
        if (cursor >= termsCount) return null;
        return term();
    }

    @Override
    public Comparator<BytesRef> getComparator() {
        return comparator;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache)
            throws IOException {
        if (termsCount == 0) {
          return SeekStatus.END;
        }
        int idx = binarySearch(termBytes, termSize, new BytesRef(termBytes, 0, termSize), new  BytesRef(termBytes,
          termSize * (termsCount - 1),
          termSize), text, comparator);
        if (idx >= 0) {
            cursor = idx;
            return SeekStatus.FOUND;
        }
        else {
            idx = -(idx + 1);
            if (idx < termsCount) {
                cursor = idx;
                return SeekStatus.NOT_FOUND;
            }
            else {
                cursor = -1;
                return SeekStatus.END;
            }
        }
    }
  public static int  binarySearch(byte[] a, int termSize, BytesRef fromIndex, BytesRef toIndex, BytesRef value,
                                   Comparator<BytesRef> comparator) {
            while (fromIndex.offset <= toIndex.offset){
                  int mid = (fromIndex.offset + toIndex.offset) / termSize >>> 1;
                  int upper = toIndex.offset;
                  toIndex.offset = mid * termSize;
                  int comp = comparator.compare(toIndex, value);
                  toIndex.offset = upper;

                  if (comp < 0) {
                    //middle is less than value
                    fromIndex.offset = (mid + 1) * termSize;
                  } else if (comp > 0)  {
                    toIndex.offset = (mid - 1) * termSize;
                  } else {
                    return mid;
                  }
              }
            return -(fromIndex.offset / termSize + 1);  // key not found.
        }



    @Override
    public void seekExact(long ord) throws IOException {
        cursor = (int)ord;
    }

    @Override
    public BytesRef term() throws IOException {
      int offset = cursor * termSize;
      return new BytesRef(termBytes, offset, termSize);
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
        return-1;
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