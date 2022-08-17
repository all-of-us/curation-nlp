/*
 * Modified from https://beam.apache.org/get-started/wordcount-example/
 */
package org.allofus.curation.pipeline;

import java.text.ParseException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.InvalidXMLException;
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

    static void runCurationNLP(CurationNLPOptions options) {
        Pipeline p = Pipeline.create(options);
        // 1. readlines from csv file
        // 2. read from multiple files in a directory
        // 3. read json
        
        p.apply(FileIO.match().filepattern(options.getInput()+"*.txt"))
            // The withCompression method is optional. By default, the Beam SDK detects compression from
            // the filename.
            .apply(FileIO.readMatches())
            //.apply(TextIO.readFiles())
            .apply(ParDo.of(new RunCLAMPFileFn()));
            //.apply(new CurationNLP());
                //ParDo.of(
                //    new DoFn<FileIO.ReadableFile, String>() {
                //        @ProcessElement
                //        public void process(@Element FileIO.ReadableFile file) {
                //        // We can now access the file and its metadata.
                //        LOG.info("File Metadata resourceId is {} ", file.getMetadata().resourceId());
                //}
                //}));        
        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
    	System.out.println("start...");
    	Instant start = Instant.now();
        CurationNLPOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);
        
		try {
			if (RunCLAMPFileFn.init_clamp(options) < 0) {
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
