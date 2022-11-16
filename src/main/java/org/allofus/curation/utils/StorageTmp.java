package org.allofus.curation.utils;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class StorageTmp {

  final Logger LOG = LoggerFactory.getLogger(StorageTmp.class);
  String bucket;
  Storage storage;
  String resources;

  public StorageTmp(String resources_dir) {
    String[] bucket_parts = resources_dir.substring(5).split("/", 2);
    bucket = bucket_parts[0];
    resources = resources_dir.substring(5).substring(bucket.length()).substring(1);
    storage = StorageOptions.getDefaultInstance().getService();
  }

  public String StoreTmpFile(String pipeline_file) throws IOException {
    Path pipeline_path = Files.createTempFile("pipeline", ".jar");
    pipeline_file = resources + "/" + pipeline_file;
    Blob blob = storage.get(BlobId.of(bucket, pipeline_file));
    blob.downloadTo(pipeline_path);
    return String.valueOf(pipeline_path);
  }

  public String StoreTmpDir(String umlsDir) throws IOException {
    Path umls_path = Files.createTempDirectory("umls");
    umlsDir = resources + "/" + umlsDir;
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
