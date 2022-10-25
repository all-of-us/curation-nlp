package io.bigquery;

import io.factory.IOWrite;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

// Sample to inserting rows into a table without running a load job.
public class BigQueryWrite extends IOWrite {

  @Override
  public PDone expand(PCollection<Row> input) {
    return null;
  }
}
// public class BigQueryWrite extends IOWrite {
//    @Override
//    public void expand(PCollection<Row> input){
//        input.apply(BigQueryIO.writeTableRows()
//                .to(tableRef)
//                .withSchema(output_schema)
//                .withCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
//                .withWriteDisposition(JobInfo.WriteDisposition.WRITE_APPEND));
//    }

// }
