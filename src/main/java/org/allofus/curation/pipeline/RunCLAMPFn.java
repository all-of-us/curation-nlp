package org.allofus.curation.pipeline;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.structure.*;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import edu.uth.clamp.nlp.omop.OMOPEncoder;
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
  static Schema output_schema = NLPSchema.getNoteNLPSchema();
  private static Map<String, String> attrMap = new HashMap<String, String>();
  private static OMOPEncoder encoder;
  File outPath;
  String resources_dir;
  String pipeline;
  File umlsIndex;
  File rxNormIndex;
  File omopIndex;
  File pipelineJar;
  String umlsIndexDir;
  String rxNormIndexDir;
  String omopIndexDir;
  String pipeline_file;

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(ParDo.of(new RunCLAMPSingleFn()))
        .setRowSchema(output_schema)
        .setCoder(SchemaCoder.of(output_schema));
  }

  public void init_clamp(CurationNLPOptions options) throws IOException {
    // Set output dir
    String outDir = options.getOutput();
    this.outPath = new File(outDir);

    // Use resources dir path and sanitize
    String resources_param = options.getResourcesDir();
    resources_dir = SanitizeInput.sanitize(resources_param);

    // Set NLP pipeline jar file to use
    this.pipeline = options.getPipeline();
    pipeline_file = "/pipeline/" + pipeline;

    // Set index dirs
    String primaryIndexDir = "/index/";
    umlsIndexDir = primaryIndexDir + "umls_index";
    rxNormIndexDir = primaryIndexDir + "rxnorm_index";
    omopIndexDir = primaryIndexDir + "omop_index";

  }

  public class RunCLAMPSingleFn extends DoFn<Row, Row> {
    private final List<DocProcessor> procList = new ArrayList<>();

    @Setup
    public void init() throws IOException, ConfigurationException, DocumentIOException {
      List<DocProcessor> pipeline;
      // If resources in google bucket, download them
      if (resources_dir.startsWith("gs")) {
        StorageTmp stmp = new StorageTmp(resources_dir);
        umlsIndexDir = stmp.StoreTmpDir(umlsIndexDir.substring(1));
        rxNormIndexDir = stmp.StoreTmpDir(rxNormIndexDir.substring(1));
        omopIndexDir = stmp.StoreTmpDir(omopIndexDir.substring(1));
        pipeline_file = stmp.StoreTmpFile(pipeline_file.substring(1));
      } else {
        umlsIndexDir = resources_dir + umlsIndexDir;
        rxNormIndexDir = resources_dir + rxNormIndexDir;
        pipeline_file = resources_dir + pipeline_file;
        omopIndexDir = resources_dir + omopIndexDir;
      }

      // Use files
      umlsIndex = new File(umlsIndexDir);
      rxNormIndex = new File(rxNormIndexDir);
      omopIndex = new File(omopIndexDir);
      pipelineJar = new File(pipeline_file);
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
      encoder = new OMOPEncoder();
      encoder.setIndexDir(omopIndexDir);

      Instant end = Instant.now();
      Duration timeElapsed = Duration.between(start, end);
      LOG.info("init CLAMP: Time taken: " + timeElapsed.toMillis() + " milliseconds");
    }

    @ProcessElement
    public void processElement(@Element Row input, OutputReceiver<Row> receiver) {
      System.out.println("RunCLAMPSingleFn: processElement...");
      try {
        String note_id = Objects.requireNonNull(input.getValue("note_id")).toString();
        String text = input.getValue("note_text");
        System.out.println("processElement: " + text);
        Document doc = new Document(note_id, text);
        for (DocProcessor proc : procList) {
          try {
            proc.process(doc);
          } catch (AnalysisEngineProcessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
        }
        System.out.println("note_id: " + note_id + " cne count: " + String.valueOf(doc.getNameEntity().size())
            + " text: " + doc.getFileContent());
        Date date = new Date();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String nlpDate = dateFormat.format(date);
        SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String nlpDatetime = datetimeFormat.format(date);

        for (ClampNameEntity cne : doc.getNameEntity()) {
          String tmp = getTermTemporal(doc, cne);
          String te = getTermExists(cne);
          String tm = getTermModifiers(doc, cne);
          String snippet = getSnippet(doc, cne);
          String offset = getOffset(cne);
          int sec_id = getSectionId(doc, cne);
          int concept_id = getNoteNlpConceptId(cne);

          Row out = Row.withSchema(output_schema)
              .addValue(0L)
              .addValue(input.getValue("note_id"))
              .addValue((long) sec_id)
              .addValue(snippet)
              .addValue(offset)
              .addValue(getLexicalVariant(cne))
              .addValue((long) getNoteNlpConceptId(cne))
              .addValue((long) getNoteNlpConceptId(cne))
              .addValue("CLAMP 1.7.5")
              .addValue(nlpDate)
              .addValue(nlpDatetime)
              .addValue(te)
              .addValue(tmp)
              .addValue(tm)
              .build();
          receiver.output(out);
        }
        LOG.info("Processed document " + note_id);
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    private int getSectionId(Document doc, ClampNameEntity cne) {
      int sec_id = 0;
      for (ClampSection sec : doc.getSections()) {
        if ((cne.getBegin() >= sec.getBegin()) && (cne.getEnd() < cne.getEnd())) {
          break;
        }
        sec_id = sec_id + 1;
      }
      return sec_id;
    }

    private int getSectionConceptId(ClampSection sec) {
      return Integer.parseInt(sec.getSectionName());
    }

    private String getSnippet(Document doc, ClampNameEntity cne) {
      int s = cne.getBegin();
      int e = cne.getEnd();
      StringBuilder snippet = new StringBuilder();
      snippet.append(doc.getFileContent(), s, e);

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
      try {
        return (int) ((encoder.encode(cne.textStr(), cne.getSemanticTag())).getConcept_id());
      } catch (Exception e) {
        return 0;
      }
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

    private Map<String, String> getAttrMap(Document doc, ClampNameEntity cne) {
      Map<String, String> attrMap = new HashMap<String, String>();
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
      return attrMap;
    }

    private String getTermTemporal(Document doc, ClampNameEntity cne) {
      String term_temporal = "";
      attrMap = getAttrMap(doc, cne);
      if (attrMap.containsKey("temporal")) {
        term_temporal = attrMap.get("temporal");
      }
      return term_temporal;
    }

    private String getTermModifiers(Document doc, ClampNameEntity cne) {
      attrMap = getAttrMap(doc, cne);
      StringBuilder term_modifiers = new StringBuilder();
      for (String k : attrMap.keySet()) {
        term_modifiers.append(k).append("=[").append(attrMap.get(k)).append("], ");
      }
      term_modifiers = new StringBuilder(term_modifiers.toString().trim());
      if (term_modifiers.toString().endsWith(",")) {
        term_modifiers = new StringBuilder(term_modifiers.substring(0, term_modifiers.length() - 1));
      }

      return term_modifiers.toString();
    }
  }
}
