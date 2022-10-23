package io.bigquery;

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
