package com.senseidb.abacus.api.codec;

import org.apache.lucene.codecs.*;
import org.apache.lucene.codecs.lucene42.Lucene42Codec;
import org.apache.lucene.index.*;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.*;

public class AbacusFacetPostingsFormat extends PostingsFormat {
  public static final String ABACUS_SEGMENT_SUFFIX = "ab";
  public static final String DOC_EXTENSION = "doc";
  private PostingsFormat delegatingFormat;
  private synchronized PostingsFormat getDelegate() {
    if (delegatingFormat == null) {
      delegatingFormat = new Lucene42Codec().postingsFormat();
    }

    return delegatingFormat;
  }
  private static final String NAME = "BoboPostingsFormat";
  public AbacusFacetPostingsFormat() {
    super(NAME);

  }
  private FieldInfo[] getAbacusFieldInfos(List<FieldInfo> infos) {
    List<FieldInfo> list = new ArrayList<FieldInfo>(infos);
    Iterator<FieldInfo> it = list.iterator();
    while (it.hasNext()) {
      FieldInfo fi = it.next();
      if (fi.getIndexOptions() != FieldInfo.IndexOptions.DOCS_ONLY) {
         it.remove();
      }
    }
    return list.toArray(new FieldInfo[list.size()]);
  }
  private FieldInfo[] getLuceneFieldInfos(List<FieldInfo> infos) {
    List<FieldInfo> list = new ArrayList<FieldInfo>(infos);
    Iterator<FieldInfo> it = list.iterator();
    while (it.hasNext()) {
      FieldInfo fi = it.next();
      if (fi.getIndexOptions() == FieldInfo.IndexOptions.DOCS_ONLY) {
        it.remove();
      }
    }
    return list.toArray(new FieldInfo[list.size()]);
  }
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
    List<FieldInfo> list = new ArrayList<FieldInfo>(state.fieldInfos.size());
    for (int i = 0; i < state.fieldInfos.size(); i++) {
      list.add(state.fieldInfos.fieldInfo(i));
    }
    //specifying distinct segment suffix for abacus fields
    SegmentWriteState abacusWriteState = new SegmentWriteState(new SegmentWriteState(state.infoStream, state.directory,
      state.segmentInfo,
      new FieldInfos(getAbacusFieldInfos(list)),state.termIndexInterval, state.segDeletes, state.context), ABACUS_SEGMENT_SUFFIX);
    SegmentWriteState luceneWriteState = new SegmentWriteState(state.infoStream, state.directory,
      state.segmentInfo,
      new FieldInfos(getLuceneFieldInfos(list)),state.termIndexInterval, state.segDeletes, state.context);
    PostingsWriterBase docsWriter = null;

    boolean success = false;
    try {
      docsWriter = new AbFacetPostingsWriter(abacusWriteState);
      FieldsConsumer ret =  new CompoundFieldsConsumer(new AbFacetFieldsConsumer(abacusWriteState, docsWriter),
        getDelegate().fieldsConsumer(luceneWriteState));
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsWriter);
      }
    }

  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    List<FieldInfo> list = new ArrayList<FieldInfo>(state.fieldInfos.size());
    for (int i = 0; i < state.fieldInfos.size(); i++) {
      list.add(state.fieldInfos.fieldInfo(i));
    }
    SegmentReadState abacusReadState = new SegmentReadState(state.directory, state.segmentInfo,
      new FieldInfos(getAbacusFieldInfos(list)), state.context, state.termsIndexDivisor, ABACUS_SEGMENT_SUFFIX);
    SegmentReadState luceneReadState = new SegmentReadState(state.directory, state.segmentInfo,
      new FieldInfos(getLuceneFieldInfos(list)), state.context, state.termsIndexDivisor);
    AbFacetPostingsReader docsReader = null;
    boolean success = false;
    try {
      docsReader = new AbFacetPostingsReader(abacusReadState);
      FieldsProducer ret = new CompoundFieldsProducer(new AbFacetFieldsProducer(abacusReadState, docsReader),
        getDelegate().fieldsProducer(luceneReadState));
      success = true;
      return ret;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docsReader);
      }
    }
  }

  public static class CompoundFieldsProducer extends FieldsProducer {
    private FieldsProducer abacusProducer;
    private FieldsProducer luceneProducer;
    private Set<String> abacusFields = new HashSet<String>();
    private Set<String> luceneFields = new HashSet<String>();
    private List<String> allFields  = new ArrayList<String>();
    public CompoundFieldsProducer(FieldsProducer abacusProducer, FieldsProducer luceneProducer) {

      this.abacusProducer = abacusProducer;
      this.luceneProducer = luceneProducer;
      Iterator<String> it = abacusProducer.iterator();
      while(it.hasNext()) {
        abacusFields.add(it.next());
      }
      it = luceneProducer.iterator();
      while(it.hasNext()) {
        luceneFields.add(it.next());
      }
      allFields.addAll(abacusFields);
      allFields.addAll(luceneFields);
      Collections.sort(allFields);
    }

    @Override
    public void close() throws IOException {
      try {
        abacusProducer.close();
      } finally {
        luceneProducer.close();
      }
    }

    @Override
    public Iterator<String> iterator() {
      return allFields.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
      if (abacusFields.contains(field)) {
        return abacusProducer.terms(field);
      } else {
        return luceneProducer.terms(field);
      }
    }

    @Override
    public int size() {
      return allFields.size();
    }
  }
  public static class CompoundFieldsConsumer extends FieldsConsumer {
    private FieldsConsumer abacusConsumer;
    private FieldsConsumer luceneConsumer;

    public CompoundFieldsConsumer(FieldsConsumer abacusConsumer, FieldsConsumer luceneConsumer) {

      this.abacusConsumer = abacusConsumer;
      this.luceneConsumer = luceneConsumer;
    }
    @Override
    public TermsConsumer addField(FieldInfo field) throws IOException {
      if (field.getIndexOptions() != FieldInfo.IndexOptions.DOCS_ONLY) {
        return luceneConsumer.addField(field);
      } else {
        return abacusConsumer.addField(field);
      }
    }

    @Override
    public void close() throws IOException {
      try {
        abacusConsumer.close();
      } finally {
        luceneConsumer.close();
      }
    }
  }
}
