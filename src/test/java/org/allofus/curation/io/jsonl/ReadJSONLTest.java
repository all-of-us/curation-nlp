package org.allofus.curation.io.jsonl;

import junit.framework.TestCase;
import org.allofus.curation.io.factory.IORead;
import org.allofus.curation.io.factory.IOReadFactory;
import org.allofus.curation.utils.NLPSchema;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import static org.allofus.curation.utils.Constants.ProjectPaths.TEST_INPUT;

public class ReadJSONLTest extends TestCase {

  public void testReadJSONL() {
    TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    String input_type = "jsonl";
    Schema note_schema = NLPSchema.getNoteSchema();

    CoderRegistry cr = p.getCoderRegistry();
    cr.registerCoderForClass(Integer.class, VarIntCoder.of());
    cr.registerCoderForClass(Long.class, VarLongCoder.of());
    cr.registerCoderForClass(Float.class, DoubleCoder.of());

    IORead ioRead = IOReadFactory.create(input_type);
    ioRead.init(TEST_INPUT, input_type);

    PCollection<Row> actual = p.apply(ioRead).setRowSchema(note_schema);

    Row row_1 = Row.withSchema(note_schema)
        .addValue(Long.valueOf("1001"))
        .addValue(Long.valueOf("1"))
        .addValue("2020-10-10")
        .addValue("2020-10-10 01:01:01")
        .addValue(Long.valueOf("3522232"))
        .addValue(Long.valueOf("0"))
        .addValue("Administrative note")
        .addValue(
            "This note contains a few characters like headache.This note contains a few characters like headache. This note contains a few characters like headache")
        .addValue(Long.valueOf("1147732"))
        .addValue(Long.valueOf("4180186"))
        .addValue(Long.valueOf("1234"))
        .addValue(Long.valueOf("123456"))
        .addValue(Long.valueOf("1234567"))
        .addValue("5")
        .build();

    Row row_2 = Row.withSchema(note_schema)
        .addValue(Long.valueOf("3002"))
        .addValue(Long.valueOf("2"))
        .addValue("2020-10-10")
        .addValue("2020-10-10 01:01:01")
        .addValue(Long.valueOf("3522232"))
        .addValue(Long.valueOf("0"))
        .addValue("Administrative note")
        .addValue(
            "This is a different note that contains a few characters like acne.This is a different note that contains a few characters like acne. This is a different note that contains a few characters like acne")
        .addValue(Long.valueOf("1147732"))
        .addValue(Long.valueOf("4180186"))
        .addValue(Long.valueOf("1234"))
        .addValue(Long.valueOf("23456"))
        .addValue(Long.valueOf("234567"))
        .addValue("6")
        .build();

    Row row_3 = Row.withSchema(note_schema)
        .addValue(Long.valueOf("3004"))
        .addValue(Long.valueOf("2"))
        .addValue("2020-10-10")
        .addValue("2020-10-10 01:01:01")
        .addValue(Long.valueOf("3522232"))
        .addValue(Long.valueOf("0"))
        .addValue("Administrative note")
        .addValue(
            "I had severe cough like eight times yesterday.")
        .addValue(Long.valueOf("1147732"))
        .addValue(Long.valueOf("4180186"))
        .addValue(Long.valueOf("1234"))
        .addValue(Long.valueOf("23456"))
        .addValue(Long.valueOf("234567"))
        .addValue("6")
        .build();

    PAssert.that(actual).containsInAnyOrder(row_1, row_2, row_3);

    p.run();
  }
}
