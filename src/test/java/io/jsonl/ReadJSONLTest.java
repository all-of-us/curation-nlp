package io.jsonl;

import io.factory.IORead;
import io.factory.IOReadFactory;
import junit.framework.TestCase;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import utils.ReadSchemaFromJson;

public class ReadJSONLTest extends TestCase {

  public void testReadJSONL() {
    TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    String home_dir = System.getProperty("user.dir");
    String test_dir = home_dir + "/src/test";
    String data_dir = test_dir + "/resources/data/";
    String input_type = "jsonl";
    Schema note_schema = ReadSchemaFromJson.ReadSchema("note.json");

    IORead ioRead = IOReadFactory.create(input_type);
    ioRead.init(data_dir, input_type);

    PCollection<org.apache.beam.sdk.values.Row> actual = p.apply(ioRead);

    Row row_1 =
        Row.withSchema(note_schema)
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

    Row row_2 =
        Row.withSchema(note_schema)
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

    PAssert.that(actual).containsInAnyOrder(row_1, row_2);

    p.run();
  }
}
