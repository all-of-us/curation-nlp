

package org.allofus.curation.pipeline;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.csv.CSVRecord;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.xml.sax.SAXException;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.main.ClampLauncher;
import edu.uth.clamp.nlp.structure.ClampNameEntity;
import edu.uth.clamp.nlp.structure.ClampRelation;
import edu.uth.clamp.nlp.structure.ClampSentence;
import edu.uth.clamp.nlp.structure.ClampToken;
import edu.uth.clamp.nlp.structure.DocProcessor;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;
import edu.uth.clamp.nlp.structure.Document;


public class RunCLAMPStringFn extends DoFn<CSVRecord, String> {
    //public class RunCLAMPFn extends DoFn<String, String>  {
    private static final ReentrantLock INIT_MUTEX_LOCK = new ReentrantLock();
    private final List<DocProcessor> procList = new ArrayList<>();
    File outPath;
    private String umlsIndexDir;
    private String pipeline_file;

    public void init_clamp(CurationNLPOptions options) {
        String outDir = options.getOutput();
        this.outPath = new File(outDir);
        String project_home = System.getProperty("user.dir");
        String resources_dir = project_home + "/src/main/resources";
        this.umlsIndexDir = resources_dir + "/index/umls_index";
        this.pipeline_file = resources_dir + "/pipeline/clamp-ner.pipeline.jar";
    }

    public void init() throws ConfigurationException, DocumentIOException, IOException {
        List<DocProcessor> pipeline;
        File umlsIndex = new File(this.umlsIndexDir);
        File pipelineJar = new File(this.pipeline_file);

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
                    File index = new File(umlsIndex.getParent() + "/rxnorm_index/");
                    ((RxNormEncoderUIMA) proc).setIndex(index.getAbsolutePath());
                    procList.add(proc);
                } else if (proc instanceof MaxMatchingUmlsEncoderCovid) {
                    File index = new File(umlsIndex.getParent() + "/umls_index_lucene8_covid/");
                    ((MaxMatchingUmlsEncoderCovid) proc).setIndexDir(index.getAbsolutePath());
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
        System.out.println("init CLAMP: Time taken: " + timeElapsed.toMillis() + " milliseconds");
    }

    @ProcessElement
    public void processElement(@Element CSVRecord csvRecord, OutputReceiver<String> receiver) {
        try {
            String fileid = csvRecord.get(0);
            String text = csvRecord.get(1);
            Document doc = new Document(fileid, text);
            for (DocProcessor proc : procList) {
                try {
                    proc.process(doc);
                } catch (AnalysisEngineProcessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            FileWriter outfile = new FileWriter(this.outPath.getAbsolutePath() + File.separator + fileid);
            for (ClampNameEntity cne : doc.getNameEntity()) {
                if (cne.getUmlsCui() != null && !cne.getUmlsCui().isEmpty()) {
                    receiver.output("\tcui=" + cne.getUmlsCui());
                    outfile.write("\tcui=" + cne.getUmlsCui());
                }
                receiver.output("\tne=" + cne.textStr().replace("\r\n", " ").replace("\n", " "));
                outfile.write("\tne=" + cne.textStr().replace("\r\n", " ").replace("\n", " "));
                outfile.write("\n");
            }

            outfile.close();
        } catch (IOException | UIMAException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(receiver);
        System.out.println("document processed..");
    }
}
