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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uth.clamp.nlp.structure.*;
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
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
import org.xml.sax.SAXException;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.uth.clamp.config.ConfigUtil;
import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.config.Processor;
import edu.uth.clamp.io.DocumentIOException;
import edu.uth.clamp.nlp.encoding.MaxMatchingUmlsEncoderCovid;
import edu.uth.clamp.nlp.encoding.RxNormEncoderUIMA;
import edu.uth.clamp.nlp.main.ClampLauncher;
import edu.uth.clamp.nlp.uima.UmlsEncoderUIMA;


public class RunCLAMPStringFn extends DoFn<CSVRecord, String> {
    //public class RunCLAMPFn extends DoFn<String, String>  {
    private static final ReentrantLock INIT_MUTEX_LOCK = new ReentrantLock();
    private final List<DocProcessor> procList = new ArrayList<>();
    File outPath;
    private String umlsIndexDir;
    private String pipeline_file;
	private static final Logger log = LoggerFactory.getLogger(Processor.class);
	private Map<String, String> attrMap = null;

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
			String noteid = csvRecord.get(0);
			String text = csvRecord.get(9);
            Document doc = new Document(noteid, text);
            for (DocProcessor proc : procList) {
                try {
                    proc.process(doc);
                } catch (AnalysisEngineProcessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
			writeResult(doc, noteid, receiver);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println(receiver);
        System.out.println("document processed..");
    }
	public void writeResult(Document doc, String noteId, OutputReceiver<String> receiver) throws Exception { // DocumentIOException {
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
		String nlpDate= dateFormat.format(date);
		try {
			for (ClampSection sec : doc.getSections()) {
				for (ClampNameEntity cne : XmiUtil.selectNE(doc.getJCas(), sec.getBegin(), sec.getEnd())) {
					String strRowEscaped = StringEscapeUtils.escapeCsv(noteId+","+noteId+","+"1"+","+getSnippet(doc, sec, cne)+","+getOffset(cne)+","+
							getLexicalVariant(cne)+","+getNoteNlpConceptId(cne)+","+"1"+","+"CLAMP 1.7.1"+","+
							nlpDate+","+nlpDate+","+getTermTemporal(doc,cne)+","+
							getTermModifiers(cne)+","+getTermExists(cne));
					System.out.println(strRowEscaped);
					receiver.output(strRowEscaped);
					/*new HashMap<String, Object>(){{
					        	put("note_nlp_id", noteId);
								put("noteid", noteId);
								put("section_concept_id", 1);
								put("snippet", getSnippet(doc, sec, cne));
								put("offset", getOffset(cne));
								put("lexical_variant", getLexicalVariant(cne));
								put("note_nlp_concept_id", getNoteNlpConceptId(cne));
								put("note_nlp_source_concept_id", 1);
								put("nlp_system", "CLAMP 1.7.1");
								put("nlp_date", nlpDate);
								put("nlp_datetime", nlpDate);
								put("term_temporal", getTermTemporal(doc, cne));
								put("term_modifiers", getTermModifiers(cne));
								put("term_exists", getTermExists(cne));
					}}*/
				}
			}
			//runTableInsertRowsWithoutRowIds(rowContent);
		} catch (Exception e) {
			log.error("Error while attempting to write results : " + e.getMessage());
			e.printStackTrace();
			throw new Exception();
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
		String snippet = "";
		for (ClampToken t : XmiUtil.selectToken(doc.getJCas(), s, e)) {
			snippet += t.textStr() + " ";
		}
		snippet = snippet.trim();
		return snippet;
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
		if (attrMap.containsKey("SUB") && !attrMap.get("SUB").toLowerCase().contains("patient")
				&& !attrMap.get("SUB").toLowerCase().contains("pt")) {
			term_exists = false;
		}
		return String.valueOf(term_exists);
	}

	private String getTermTemporal(Document doc, ClampNameEntity cne) {
		attrMap = new HashMap<String, String>();
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
		String term_modifiers = "";
		for (String k : attrMap.keySet()) {
			term_modifiers += k + "=[" + attrMap.get(k) + "], ";
		}
		term_modifiers = term_modifiers.trim();
		if (term_modifiers.endsWith(",")) {
			term_modifiers = term_modifiers.substring(0, term_modifiers.length() - 1);
		}

		return term_modifiers;
	}
}
