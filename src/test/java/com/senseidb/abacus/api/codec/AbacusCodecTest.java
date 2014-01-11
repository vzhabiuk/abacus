package com.senseidb.abacus.api.codec;

import com.senseidb.abacus.api.codec.term.ConversionUtils;
import com.senseidb.abacus.api.codec.utils.AbacusUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class AbacusCodecTest {
  private IndexWriter writer;
  private RAMDirectory dir;
  private DirectoryReader reader;
  private int numdocs;
  private IndexSearcher searcher;

  @Before
  public void setUpIndex() throws Exception {
    IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_44, new StandardAnalyzer(Version.LUCENE_44));
    // writerConfig.setCodec(new Lucene42Codec());
    writerConfig.setCodec(new AbacusCodec());
    numdocs = 1000;
    dir = new RAMDirectory();

    writer = new IndexWriter(dir, writerConfig);
    FieldType ft = getFieldType();
    for (int i = 0; i < numdocs; ++i) {
      Document doc = new Document();
      doc.add(new NumericDocValuesField("id", i));
      doc.add(AbacusUtils.field("tokens", i, ft));
      doc.add(AbacusUtils.field("longVal", (long)i % 10, ft));
      doc.add(new Field("strVal", "test" + ((i % 2 == 0) ? "pair" + (i % 100) : "" + (i % 100)), ft));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    writer.commit();
    writer.close();
    reader = DirectoryReader.open(dir);
    searcher = new IndexSearcher(reader);
  }
  @After
  public void cleanup() throws Exception {
    reader.close();
  }
  @Test
  public void test1IntTermQuery() throws Exception {
    TermQuery q1 = new TermQuery(new Term("tokens", new BytesRef(ConversionUtils.fromInt(4))));
    TopDocs td1 = searcher.search(q1, 1000);
    assertEquals(td1.totalHits, 1);
  }
  @Test
  public void test2LongTermQuery() throws Exception {
    TermQuery q1 = new TermQuery(new Term("longVal", new BytesRef(ConversionUtils.fromLong(4))));
    TopDocs td1 = searcher.search(q1, 1000);
    assertEquals(td1.totalHits, 100);
  }
  @Test
  public void test3IntRangeQuery() throws Exception {
    TermRangeQuery q2 = new TermRangeQuery("tokens", new BytesRef(ConversionUtils.fromInt(20)),
      new BytesRef(ConversionUtils.fromInt(1020)),
      true,
      true);
    TopDocs td2 = searcher.search(q2, 1000);
    assertEquals(td2.totalHits, 980);
  }
  @Test
  public void test4LongRangeQuery() throws Exception {
    TermRangeQuery q2 = new TermRangeQuery("longVal", new BytesRef(ConversionUtils.fromLong(5)),
      new BytesRef(ConversionUtils.fromLong(20)),
      true,
      true);
    TopDocs td2 = searcher.search(q2, 1000);
    assertEquals(td2.totalHits, 500);
  }
  @Test
  public void test5IterateTermEnum() throws Exception {
    AtomicReader atomicReader = reader.leaves().get(0).reader();
    Terms terms = atomicReader.terms("tokens");
    TermsEnum iterator = terms.iterator(null);
    BytesRef term;
    int i = 0;

    while ((term = iterator.next()) != null) {
      assertEquals(ConversionUtils.toInt(term.bytes, term.offset), i);
      assertEquals(term.length, 4);
      i++;
    }
    assertEquals(1000, i);
  }
  @Test
  public void test6StringTermQuery() throws Exception {
    TermQuery q1 = new TermQuery(new Term("strVal", "test3"));
    TopDocs td1 = searcher.search(q1, 1000);
    assertEquals(td1.totalHits, 10);
  }
  private FieldType getFieldType() {
    FieldType ft = new FieldType();
    ft.setIndexed(true);
    ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
    ft.setTokenized(true);
    ft.setOmitNorms(true);
    ft.setStoreTermVectors(false);
    ft.setStored(false);
    return ft;
  }
  //private static AbacusCodec codec = new AbacusCodec();
}
