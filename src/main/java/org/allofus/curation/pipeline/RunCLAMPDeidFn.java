package org.allofus.curation.pipeline;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.omop.OMOPEncoder;
import edu.uth.clamp.nlp.structure.*;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import org.allofus.curation.deid.DeidentificationAouConf;
import org.allofus.curation.deid.DeidentificationAouProc;
import edu.columbia.dbmi.utils.NLPSchema;
import edu.columbia.dbmi.utils.SanitizeInput;
import edu.columbia.dbmi.utils.StorageTmp;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunCLAMPDeidFn extends RunCLAMPBaseFn {

  private static final ReentrantLock INIT_MUTEX_LOCK = new ReentrantLock();
  private static final Logger LOG = LoggerFactory.getLogger(RunCLAMPDeidFn.class);
  private static final Map<String, String> attrMap = new HashMap<String, String>();
  static Schema output_schema = NLPSchema.getNoteSchema();
  private static OMOPEncoder encoder;
  File outPath;
  String resources_dir;
  String pipeline;
  File umlsIndex;
  File rxNormIndex;
  File omopIndex;
  File deidFolder;
  File pipelineJar;
  String umlsIndexDir;
  String rxNormIndexDir;
  String omopIndexDir;
  String deidDir;
  String pipeline_file;
  Integer maxClampThreads;
  DeidentificationAouProc deidProcessor;

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
      .apply(ParDo.of(new RunCLAMPDeidSingleFn()))
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
    deidDir = primaryIndexDir + "deid_dir";

    // set numThread
    maxClampThreads = options.getMaxClampThreads();
  }

  public class RunCLAMPDeidSingleFn extends DoFn<Row, Row> {
    private final List<DocProcessor> procList = new ArrayList<>();

    @Setup
    public void init()
      throws IOException, UIMAException, DocumentIOException, ConfigurationException {
      List<DocProcessor> pipeline;
      // If resources in google bucket, download them
      if (resources_dir.startsWith("gs")) {
        StorageTmp stmp = new StorageTmp(resources_dir);
        umlsIndexDir = stmp.StoreTmpDir(umlsIndexDir.substring(1));
        rxNormIndexDir = stmp.StoreTmpDir(rxNormIndexDir.substring(1));
        omopIndexDir = stmp.StoreTmpDir(omopIndexDir.substring(1));
        deidDir = stmp.StoreTmpDir(deidDir.substring(1));
        pipeline_file = stmp.StoreTmpFile(pipeline_file.substring(1));
      } else {
        umlsIndexDir = resources_dir + umlsIndexDir;
        rxNormIndexDir = resources_dir + rxNormIndexDir;
        omopIndexDir = resources_dir + omopIndexDir;
        deidDir = resources_dir + deidDir;
        pipeline_file = resources_dir + pipeline_file;
      }

      // Use files
      umlsIndex = new File(umlsIndexDir);
      rxNormIndex = new File(rxNormIndexDir);
      omopIndex = new File(omopIndexDir);
      deidFolder = new File(deidDir);
      pipelineJar = new File(pipeline_file);
      Instant start = Instant.now();
      try {
        INIT_MUTEX_LOCK.lock();
        // load pipelines;
        pipeline = ConfigUtil.importPipelineFromJar(pipelineJar);
        DeidentificationAouConf conf = new DeidentificationAouConf(deidFolder.toPath(),
          60,
          true);
        deidProcessor = conf.createProcessor();

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
      LOG.info("init CLAMP De-id: Time taken: " + timeElapsed.toMillis() + " milliseconds");
    }

    @ProcessElement
    public void processElement(@Element Row input, OutputReceiver<Row> receiver) {
      try {
        String note_id = Objects.requireNonNull(input.getValue("note_id")).toString();
        Long person_id = input.getValue("person_id");
        String note_date = input.getValue("note_date");
        String note_datetime = input.getValue("note_datetime");
        Long note_type_concept_id = input.getValue("note_type_concept_id");
        Long note_class_concept_id = input.getValue("note_class_concept_id");
        String note_title = input.getValue("note_title");
        String note_text = input.getValue("note_text");
        Long encoding_concept_id = input.getValue("encoding_concept_id");
        Long language_concept_id = input.getValue("language_concept_id");
        Long provider_id = input.getValue("provider_id");
        Long visit_occurrence_id = input.getValue("visit_occurrence_id");
        Long visit_detail_id = input.getValue("visit_detail_id");
        String note_source_value = input.getValue("note_source_value");
        Document doc = new Document(note_id, note_text);
        ExecutorService clampExecutor = Executors.newSingleThreadExecutor();
        FutureTask<Throwable> future = new FutureTask<>(() -> {
          try {
            for (DocProcessor proc : procList) {
              proc.process(doc);
            }

            String deid_text = deidProcessor.process(doc);

            Row out = Row.withSchema(output_schema)
              .addValue(Long.valueOf(note_id))
              .addValue(person_id)
              .addValue(note_date)
              .addValue(note_datetime)
              .addValue(note_type_concept_id)
              .addValue(note_class_concept_id)
              .addValue(note_title)
              .addValue(deid_text)
              .addValue(encoding_concept_id)
              .addValue(language_concept_id)
              .addValue(provider_id)
              .addValue(visit_occurrence_id)
              .addValue(visit_detail_id)
              .addValue(note_source_value)
              .build();
            receiver.output(out);
            return null;
          } catch (AnalysisEngineProcessException e) {
            return e;
          }
        });
        clampExecutor.submit(future);
        try {
          Throwable t = future.get(120, TimeUnit.SECONDS);
          if (t != null) {
            throw new RuntimeException(t);
          }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
          System.out.println("Skipping document " + note_id + " since run taking too long.");
          future.cancel(true);
          clampExecutor.shutdownNow();
          receiver.output(emptyOutputRow(
            note_id,
            person_id,
            note_date,
            note_datetime,
            note_type_concept_id,
            note_class_concept_id,
            note_title,
            encoding_concept_id,
            language_concept_id,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            note_source_value));
        } catch (Throwable t) {
          System.out.println("Skipping document " + note_id + " due to error.");
          future.cancel(true);
          clampExecutor.shutdownNow();
          receiver.output(emptyOutputRow(
            note_id,
            person_id,
            note_date,
            note_datetime,
            note_type_concept_id,
            note_class_concept_id,
            note_title,
            encoding_concept_id,
            language_concept_id,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            note_source_value));
        }
      } catch (Exception e) {
        e.printStackTrace();
        receiver.output(emptyOutputRow("0", 0L, "0", "0", 0L,
          0L, "0", 0L, 0L, 0L,
          0L, 0L, "0"));
      }
    }

    private Row emptyOutputRow(
      String note_id,
      Long person_id,
      String note_date,
      String note_datetime,
      Long note_type_concept_id,
      Long note_class_concept_id,
      String note_title,
      Long encoding_concept_id,
      Long language_concept_id,
      Long provider_id,
      Long visit_occurrence_id,
      Long visit_detail_id,
      String note_source_value) {
      Row out = Row.withSchema(output_schema)
        .addValue(Long.valueOf(note_id))
        .addValue(Long.valueOf(person_id))
        .addValue(note_date)
        .addValue(note_datetime)
        .addValue(note_type_concept_id)
        .addValue(Long.valueOf(note_class_concept_id))
        .addValue(note_title)
        .addValue("Deid failure")
        .addValue(Long.valueOf(encoding_concept_id))
        .addValue(Long.valueOf(language_concept_id))
        .addValue(Long.valueOf(provider_id))
        .addValue(Long.valueOf(visit_occurrence_id))
        .addValue(Long.valueOf(visit_detail_id))
        .addValue(note_source_value)
        .build();
      return out;
    }
  }
}
