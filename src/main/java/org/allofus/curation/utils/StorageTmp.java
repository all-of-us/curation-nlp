package org.allofus.curation.utils;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.allofus.curation.utils.Constants.Env.GOOGLE_APPLICATION_CREDENTIALS;
import static org.allofus.curation.utils.Constants.Env.PROJECT_ID;

public class StorageTmp {

  String bucket;
  Storage storage;

  public StorageTmp(String resources_dir) throws IOException {
    String[] bucket_parts = resources_dir.substring(5).split("/", 2);
    bucket = bucket_parts[0];
    storage =
        StorageOptions.newBuilder()
            .setProjectId(PROJECT_ID)
            .setCredentials(
                GoogleCredentials.fromStream(
                    Files.newInputStream(Paths.get(GOOGLE_APPLICATION_CREDENTIALS))))
            .build()
            .getService();
  }

  public String StoreTmpFile(String pipeline_file) throws IOException {
    Path pipeline_path = Files.createTempFile("pipeline", ".jar");
    Blob blob = storage.get(BlobId.of(bucket, pipeline_file));
    blob.downloadTo(pipeline_path);
    return String.valueOf(pipeline_path);
  }

  public String StoreTmpDir(String umlsDir) throws IOException {
    Path umls_path = Files.createTempDirectory("umls");
    Page<Blob> blobs = storage.list(bucket, Storage.BlobListOption.prefix(umlsDir));
    for (Blob blob : blobs.iterateAll()) {
      String blob_name = blob.getName();
      if (!blob_name.endsWith("/")) {
        blob_name = blob_name.substring(blob.getName().lastIndexOf("/"));
        File file_path = new File(umls_path + blob_name);
        blob.downloadTo(file_path.toPath());
      }
    }

    return String.valueOf(umls_path);
  }
}
