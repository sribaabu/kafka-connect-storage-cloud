package io.confluent.connect.s3.backup;

import io.confluent.connect.s3.storage.S3OutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.storage.backup.StorageWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * S3 implementation of {@link StorageWriter}.
 *
 * <p>Handles S3's commit-before-close semantics:
 * S3OutputStream requires commit() to finalize
 * multipart uploads; close() alone aborts them.
 *
 * <p>Extend this pattern for GCS, Azure, HDFS by
 * implementing StorageWriter for each backend.
 */
public class S3StorageWriter implements StorageWriter {

  private static final Logger log =
      LoggerFactory.getLogger(S3StorageWriter.class);

  private final S3Storage storage;

  public S3StorageWriter(S3Storage storage) {
    this.storage = storage;
  }

  @Override
  public void write(String path, String content) {
    try {
      OutputStream out = storage.create(
          path, storage.conf(), true);
      S3OutputStream s3out = (S3OutputStream) out;
      s3out.write(content.getBytes(StandardCharsets.UTF_8));
      s3out.commit();
      s3out.close();
    } catch (IOException e) {
      throw new ConnectException(
          "Failed to write to S3: " + path, e);
    }
  }

  @Override
  public boolean exists(String path) {
    return storage.exists(path);
  }

  @Override
  public byte[] read(String path) {
    try {
      org.apache.avro.file.SeekableInput input =
          storage.open(path, storage.conf());
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buf = new byte[4096];
      int read;
      while ((read = input.read(buf, 0, buf.length)) > 0) {
        baos.write(buf, 0, read);
      }
      input.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new ConnectException(
          "Failed to read from S3: " + path, e);
    }
  }
}
