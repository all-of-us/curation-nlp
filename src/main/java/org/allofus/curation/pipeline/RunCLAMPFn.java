package org.allofus.curation.pipeline;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.structure.*;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import org.allofus.curation.utils.NLPSchema;
import org.allofus.curation.utils.SanitizeInput;
import org.allofus.curation.utils.StorageTmp;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class RunCLAMPFn extends PTransform<PCollection<Row>, PCollection<Row>> {

  private static final ReentrantLock INIT_MUTEX_LOCK = new ReentrantLock();
  private static final Logger LOG = LoggerFactory.getLogger(RunCLAMPFn.class);
  private static final List<DocProcessor> procList = new ArrayList<>();
  static Schema output_schema = NLPSchema.getNoteNLPSchema();
  private static Map<String, String> attrMap = new HashMap<String, String>();
  File outPath;
  String resources_dir;
  String pipeline;

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(ParDo.of(new RunCLAMPSingleFn()))
        .setRowSchema(output_schema)
        .setCoder(SchemaCoder.of(output_schema));
  }

  public void init_clamp(CurationNLPOptions options) {
    String outDir = options.getOutput();
    this.outPath = new File(outDir);
    String resources_param = options.getResourcesDir();
    resources_dir = SanitizeInput.sanitize(resources_param);
    this.pipeline = options.getPipeline();
  }

  public class RunCLAMPSingleFn extends DoFn<Row, Row> {

    StorageTmp stmp = new StorageTmp(resources_dir);

    @Setup
    public void init() throws IOException, ConfigurationException, DocumentIOException {
      String primaryIndexDir = "/index/";
      String umlsIndexDir = primaryIndexDir + "umls_index";
      String rxNormIndexDir = primaryIndexDir + "rxnorm_index";
      String pipeline_file = "/pipeline/" + pipeline;

      List<DocProcessor> pipeline;
      if (resources_dir.startsWith("gs")) {
        umlsIndexDir = stmp.StoreTmpDir(umlsIndexDir.substring(1));
        rxNormIndexDir = stmp.StoreTmpDir(rxNormIndexDir.substring(1));
        pipeline_file = stmp.StoreTmpFile(pipeline_file.substring(1));
      } else {
        umlsIndexDir = resources_dir + umlsIndexDir;
        rxNormIndexDir = resources_dir + rxNormIndexDir;
        pipeline_file = resources_dir + pipeline_file;
      }

      File umlsIndex = new File(umlsIndexDir);
      File rxNormIndex = new File(rxNormIndexDir);
      File pipelineJar = new File(pipeline_file);

      Instant start = Instant.now();
      try {
        INIT_MUTEX_LOCK.lock();
        // load pipelines;
        pipeline = ConfigUtil.importPipelineFromJar(pipelineJar);

        for (DocProcessor proc : pipeline) {
          if (proc instanceof UmlsEncoderUIMA) {
            ((UmlsEncoderUIMA) proc).setIndexDir(umlsIndex);
            procList.add(proc);
          } else if (proc instanceof RxNormEncoderUIMA) {
            ((RxNormEncoderUIMA) proc).setIndex(rxNormIndexDir);
            procList.add(proc);
          } else if (proc instanceof MaxMatchingUmlsEncoderCovid) {
            ((MaxMatchingUmlsEncoderCovid) proc).setIndexDir(umlsIndexDir);
            procList.add(proc);
          } else {
            procList.add(proc);
          }
        }
      } finally {
        INIT_MUTEX_LOCK.unlock();
      }
      Instant end = Instant.now();
      Duration timeElapsed = Duration.between(start, end);
      LOG.info("init CLAMP: Time taken: " + timeElapsed.toMillis() + " milliseconds");
    }

    @ProcessElement
    public void processElement(@Element Row input, OutputReceiver<Row> receiver) {
      try {
        String note_id = Objects.requireNonNull(input.getValue("note_id")).toString();
        String text = input.getValue("note_text");
        Document doc = new Document(note_id, text);
        for (DocProcessor proc : procList) {
          try {
            proc.process(doc);
          } catch (AnalysisEngineProcessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String nlpDate = dateFormat.format(date);
        int sec_id = 0;
        for (ClampSection sec : doc.getSections()) {
          for (ClampNameEntity cne : doc.getNameEntity()) {
            String tmp = getTermTemporal(doc, cne);
            String te = getTermExists(cne);
            String tm = getTermModifiers(cne);
            Row out =
                Row.withSchema(output_schema)
                    .addValue(0L)
                    .addValue(input.getValue("note_id"))
                    .addValue((long) sec_id)
                    .addValue(getSnippet(doc, sec, cne))
                    .addValue(getOffset(cne))
                    .addValue(getLexicalVariant(cne))
                    .addValue((long) getNoteNlpConceptId(cne))
                    .addValue((long) getNoteNlpConceptId(cne))
                    .addValue("CLAMP 1.7.2")
                    .addValue(nlpDate)
                    .addValue(nlpDate)
                    .addValue(te)
                    .addValue(tmp)
                    .addValue(tm)
                    .build();
            receiver.output(out);
          }
          sec_id++;
        }
        LOG.info("Processed document " + note_id);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    private int getSectionConceptId(ClampSection sec) {
      return Integer.parseInt(sec.getSectionName());
    }

    private String getSnippet(Document doc, ClampSection sec, ClampNameEntity cne) {
      int s = cne.getBegin();
      int e = cne.getEnd();
      s = Math.max(sec.getBegin(), s);
      e = Math.min(sec.getEnd(), e);
      StringBuilder snippet = new StringBuilder();
      for (ClampToken t : XmiUtil.selectToken(doc.getJCas(), s, e)) {
        snippet.append(t.textStr()).append(" ");
      }
      snippet = new StringBuilder(snippet.toString().trim());
      return snippet.toString();
    }

    private String getOffset(ClampNameEntity cne) {
      return cne.getBegin() + "-" + cne.getEnd();
    }

    private String getLexicalVariant(ClampNameEntity cne) {
      return cne.textStr();
    }

    private int getNoteNlpConceptId(ClampNameEntity cne) {
      return 0;
    }

    private String getTermExists(ClampNameEntity cne) {
      Boolean term_exists = true;
      if (cne.getAssertion() != null && cne.getAssertion().equals("absent")) {
        term_exists = false;
      }
      if (attrMap.containsKey("CON")) {
        term_exists = false;
      }
      if (attrMap.containsKey("SUB")
          && !attrMap.get("SUB").toLowerCase().contains("patient")
          && !attrMap.get("SUB").toLowerCase().contains("pt")) {
        term_exists = false;
      }
      return String.valueOf(term_exists);
    }

    private String getTermTemporal(Document doc, ClampNameEntity cne) {
      String term_temporal = "";
      for (ClampRelation rel : doc.getRelations()) {
        ClampNameEntity t = null;
        if (rel.getEntFrom().getUimaEnt().equals(cne.getUimaEnt())) {
          t = rel.getEntTo();
        } else if (rel.getEntTo().getUimaEnt().equals(cne.getUimaEnt())) {
          t = rel.getEntFrom();
        }
        if (t == null) {
          continue;
        }
        String k = t.getSemanticTag();
        if (k.contains(":")) {
          k = k.substring(k.lastIndexOf(":") + 1);
        }
        attrMap.putIfAbsent(k, "");
        attrMap.put(k, (attrMap.get(k) + " " + t.textStr()).trim());
      }
      if (attrMap.containsKey("temporal")) {
        term_temporal = attrMap.get("temporal");
      }
      return term_temporal;
    }

    private String getTermModifiers(ClampNameEntity cne) {
      StringBuilder term_modifiers = new StringBuilder();
      for (String k : attrMap.keySet()) {
        term_modifiers.append(k).append("=[").append(attrMap.get(k)).append("], ");
      }
      term_modifiers = new StringBuilder(term_modifiers.toString().trim());
      if (term_modifiers.toString().endsWith(",")) {
        term_modifiers =
            new StringBuilder(term_modifiers.substring(0, term_modifiers.length() - 1));
      }

      return term_modifiers.toString();
    }
  }
}
