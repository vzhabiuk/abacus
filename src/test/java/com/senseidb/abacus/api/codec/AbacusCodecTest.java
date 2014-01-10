package com.senseidb.abacus.api.codec;

import junit.framework.TestCase;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.junit.Test;



public class AbacusCodecTest {


    @Test
    public void testCodecBasic() throws Exception{
        IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_44, new StandardAnalyzer(Version.LUCENE_44));

        writerConfig.setCodec(new AbacusCodec());
        int numdocs = 1000;
        RAMDirectory dir = new RAMDirectory();

        IndexWriter writer = new IndexWriter(dir, writerConfig);

        for (int i = 0; i < numdocs; ++i) {
            Document doc = new Document();
            FieldType ft = new FieldType();
            ft.setIndexed(true);
            ft.setStored(true);
            ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_ONLY);
            ft.setOmitNorms(true);
            ft.setTokenized(false);
            ft.setNumericType(FieldType.NumericType.INT);
            Field f = new IntField("num", i,ft);
            doc.add(f);
            writer.addDocument(doc);
        }

        writer.commit();
        writer.close();

        IndexReader reader = DirectoryReader.open(dir);
        System.out.println(reader.document(2).getFields());
        AtomicReader atomicReader = reader.leaves().get(0).reader();
        Terms terms = atomicReader.terms("num");
        TermsEnum iterator = terms.iterator(null);
        BytesRef term;
        while ((term = iterator.next()) != null) {
            //System.out.println("term = " + term + " offset = " + term.offset + " size = " + term.length);
        }
        IndexSearcher searcher = new IndexSearcher(reader);
        BytesRef bytes = new BytesRef(NumericUtils.BUF_SIZE_INT);
        NumericUtils.intToPrefixCoded(1, 0, bytes);
        TermQuery q1 = new TermQuery(new Term("num", bytes));
        NumericRangeQuery<Integer> q2 = NumericRangeQuery.newIntRange("num", 11, 234, true, true);
        TopDocs td1 = searcher.search(q1, 1000);
        System.out.println(td1.totalHits);
        TopDocs td2 = searcher.search(q2, 1000);
        System.out.println(td2.totalHits);
        System.out.println(td1.scoreDocs.length);
        TestCase.assertEquals(numdocs, reader.numDocs());

        reader.close();
    }

    // private static AbacusCodec codec = new AbacusCodec();
}
