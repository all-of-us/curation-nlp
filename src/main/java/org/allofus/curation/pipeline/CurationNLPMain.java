/*
 * Modified from https://beam.apache.org/get-started/wordcount-example/
 */
package org.allofus.curation.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
//import org.apache.uima.resource.ResourceInitializationException;
//import org.apache.uima.util.InvalidXMLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;


/**
 * Uses Beam WordCount example that counts words in text.
 *
 * <pre>
 *   1. Execute a Pipeline both locally and using the selected runner
 *   2. Use ParDo with static DoFns defined out-of-line
 *   3. Build a composite transform
 *   4. Define pipeline options
 * </pre>
 */
public class CurationNLPMain {
	private static final Logger LOG = LoggerFactory.getLogger(CurationNLPMain.class);
	private enum Header {
		id, text;
	}
	
    static void runCurationNLP(CurationNLPOptions options) {
        Pipeline p = Pipeline.create(options);
        if (options.getInputExt().equals("txt")) {
        	// read from multiple txt files in input directory
        	p.apply(FileIO.match().filepattern((options.getInput()+"*.txt")))
        		.apply(FileIO.readMatches())
        		.apply(ParDo.of(new RunCLAMPFileFn()));
        } else if (options.getInputExt().equalsIgnoreCase("csv")) {
        	// readlines from csv file
            p.apply(FileIO.match().filepattern(options.getInput()+"*.csv")) //PCollection<Metadata>
				.apply(FileIO.readMatches()) //PCollection<ReadableFile>
				.apply(ParDo.of(
						new DoFn<FileIO.ReadableFile, CSVRecord>() {
							@ProcessElement
							public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<CSVRecord> receiver) throws IOException {
								InputStream is = Channels.newInputStream(element.open());
								Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
								Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(Header.class).withDelimiter(',').withFirstRecordAsHeader().parse(reader);
								for (CSVRecord record : records) { receiver.output(record); }
							}}
				))
                // The withCompression method is optional. By default, the Beam SDK detects compression from
                // the filename.
                .apply(ParDo.of(new RunCLAMPStringFn()));
        } else if (options.getInputExt().equalsIgnoreCase("json")) {
        	// read json
        }
        
        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
    	System.out.println("start...");
    	Instant start = Instant.now();
        CurationNLPOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);
        
		try {
			if (RunCLAMPStringFn.init_clamp(options) < 0) {
			    System.out.println("init CLAMP pipeline failed. Please check the input parameters.");
		        //RunCLAMPPipeline.run_pipeline();
			} else {
				Instant start2 = Instant.now();
		    	runCurationNLP(options);
		    	Instant end2 = Instant.now();
		    	Duration timeElapsed = Duration.between(start2, end2);
		    	System.out.println("runCurationNLP: Time taken: "+ timeElapsed.toMillis() +" milliseconds");    	
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	Instant end = Instant.now();
    	Duration timeElapsed = Duration.between(start, end);
    	System.out.println("Time taken: "+ timeElapsed.toMillis() +" milliseconds");    	

        System.out.println("Run CLAMP pipeline done.");    	

        //runWordCount(options);

        System.exit(0);
    }
}
