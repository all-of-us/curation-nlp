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
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;


public class CurationNLPMain {
    private static final Logger LOG = LoggerFactory.getLogger(CurationNLPMain.class);

    static void runCurationNLP(CurationNLPOptions options) {
        Pipeline p = Pipeline.create(options);
		RunCLAMPStringFn clamp_str_fn = new RunCLAMPStringFn();
		clamp_str_fn.init_clamp(options);
		p.apply(FileIO.match().filepattern(options.getInput() + "*.csv"))
			.apply(FileIO.readMatches())
			.apply(ParDo.of(
					new DoFn<FileIO.ReadableFile, CSVRecord>() {
						@ProcessElement
						public void processElement(@Element FileIO.ReadableFile element, OutputReceiver<CSVRecord> receiver) throws IOException {
							InputStream is = Channels.newInputStream(element.open());
							Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
							Iterable<CSVRecord> records = CSVFormat.DEFAULT.withHeader(Header.class).withDelimiter(',').withFirstRecordAsHeader().parse(reader);
							for (CSVRecord record : records) {
								receiver.output(record);
							}
						}
					}
			))
			.apply(ParDo.of(clamp_str_fn))
			.apply(TextIO.write()
				   .to(options.getOutput()+"/note_output")
				   .withHeader("note_id,")
				   .withoutSharding()
				   .withSuffix(".csv")
			 );

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        System.out.println("start...");
        Instant start = Instant.now();
        CurationNLPOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(CurationNLPOptions.class);

        Instant start2 = Instant.now();
        runCurationNLP(options);
        Instant end2 = Instant.now();
        Duration timeElapsed = Duration.between(start2, end2);
        System.out.println("runCurationNLP: Time taken: " + timeElapsed.toMillis() + " milliseconds");
        Instant end = Instant.now();
        timeElapsed = Duration.between(start, end);
        System.out.println("Time taken: " + timeElapsed.toMillis() + " milliseconds");
        System.out.println("Run CLAMP pipeline done.");

        System.exit(0);
    }

    private enum Header {
        id, text
    }
}
