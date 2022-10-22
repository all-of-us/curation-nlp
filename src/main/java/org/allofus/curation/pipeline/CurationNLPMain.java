package org.allofus.curation.pipeline;

import edu.uth.clamp.config.ConfigurationException;
import edu.uth.clamp.io.DocumentIOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.ReadSchemaFromJson;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;


public class CurationNLPMain {
    private static final Logger LOG = LoggerFactory.getLogger(CurationNLPMain.class);

    static void runCurationNLP(CurationNLPOptions options) throws ConfigurationException, DocumentIOException, IOException {
        Pipeline p = Pipeline.create(options);
        RunCLAMPFn clamp_str_fn = new RunCLAMPFn();
        clamp_str_fn.init_clamp(options);
        Schema note_schema = ReadSchemaFromJson.ReadSchema("note.json");
        Schema note_nlp_schema = ReadSchemaFromJson.ReadSchema("note_nlp.json");
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
                )).apply(ParDo.of(
                        new DoFn<CSVRecord, Row>() {
                            @ProcessElement
                            public void processElement(@Element CSVRecord element, OutputReceiver<Row> receiver) {
                                Schema schema = ReadSchemaFromJson.ReadSchema("note.json");
                                Row output = Row.withSchema(schema)
                                        .addValue(Long.valueOf(element.get(0)))
                                        .addValue(Long.valueOf(element.get(1)))
                                        .addValue(DateTimeFormat.forPattern("yyyy-MM-dd").parseDateTime(element.get(2)))
                                        .addValue(DateTimeFormat.forPattern("yyyy-MM-dd hh:mm:ss").parseDateTime(element.get(3)))
                                        .addValue(Long.valueOf(element.get(4)))
                                        .addValue(Long.valueOf(element.get(5)))
                                        .addValue(element.get(6))
                                        .addValue(element.get(7))
                                        .addValue(Long.valueOf(element.get(8)))
                                        .addValue(Long.valueOf(element.get(9)))
                                        .addValue(Long.valueOf(element.get(10)))
                                        .addValue(Long.valueOf(element.get(11)))
                                        .addValue(Long.valueOf(element.get(12)))
                                        .addValue(element.get(13)).build();
                                receiver.output(output);
                            }
                        }
                )).setCoder(SchemaCoder.of(note_schema)).setRowSchema(note_schema)
                .apply(ParDo.of(clamp_str_fn)).setCoder(SchemaCoder.of(note_nlp_schema)).setRowSchema(note_nlp_schema)
                .apply("Serialize to Json", ToJson.of())
                .apply(TextIO.write()
                        .to(options.getOutput() + "output")
                        .withSuffix(".jsonl")
                );

        p.run().waitUntilFinish();
    }

    public static void main(String[] args) throws ConfigurationException, DocumentIOException, IOException {
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
