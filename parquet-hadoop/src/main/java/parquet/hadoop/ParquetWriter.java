/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.hadoop;

import java.io.Closeable;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.schema.MessageType;

/**
 * Write records to a Parquet file.
 */
public class ParquetWriter<T> implements Closeable {

  public static final int DEFAULT_BLOCK_SIZE = 128 * 1024 * 1024;
  public static final int DEFAULT_PAGE_SIZE = 1 * 1024 * 1024;

  private final InternalParquetRecordWriter<T> writer;
  private Map<String,String> protectedMetaData;
  private Map<String,String> fileMetaData;

  /**
   * Create a new ParquetWriter.
   * (with dictionary encoding disabled and validation off)
   * @see ParquetWriter#ParquetWriter(Path, WriteSupport, CompressionCodecName, int, int, boolean)
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @throws IOException
   */
  public ParquetWriter(Path file, WriteSupport<T> writeSupport, CompressionCodecName compressionCodecName, int blockSize, int pageSize) throws IOException {
    this(file, writeSupport, compressionCodecName, blockSize, pageSize, false, false);
  }

  /**
   * Create a new ParquetWriter.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @param compressionCodecName the compression codec to use
   * @param blockSize the block size threshold
   * @param pageSize the page size threshold
   * @param enableDictionary to turn dictionary encoding on
   * @param validating to turn on validation using the schema
   * @throws IOException
   */
  public ParquetWriter(
      Path file,
      WriteSupport<T> writeSupport,
      CompressionCodecName compressionCodecName,
      int blockSize,
      int pageSize,
      boolean enableDictionary,
      boolean validating) throws IOException {
    Configuration conf = new Configuration();

    WriteSupport.WriteContext writeContext = writeSupport.init(conf);
    MessageType schema = writeContext.getSchema();

    ParquetFileWriter fileWriter = new ParquetFileWriter(conf, schema, file);
    fileWriter.start();

    CodecFactory codecFactory = new CodecFactory(conf);
    CodecFactory.BytesCompressor compressor =	codecFactory.getCompressor(compressionCodecName, 0);
    this.protectedMetaData = writeContext.getExtraMetaData();
    this.fileMetaData = new HashMap<String,String>(protectedMetaData);
    this.writer = new InternalParquetRecordWriter<T>(fileWriter, writeSupport, schema, blockSize, pageSize, compressor, enableDictionary, validating);

  }

  /**
   * Create a new ParquetWriter.  The default block size is 50 MB.The default
   * page size is 1 MB.  Default compression is no compression. Dictionary encoding is disabled.
   *
   * @param file the file to create
   * @param writeSupport the implementation to write a record to a RecordConsumer
   * @throws IOException
   */
  public ParquetWriter(Path file, WriteSupport<T> writeSupport) throws IOException {
    this(file, writeSupport, CompressionCodecName.UNCOMPRESSED, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
  }

  public void write(T object) throws IOException {
    try {
      writer.write(object);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public boolean containsFileMetaDataKey(String key) {
    return fileMetaData.containsKey(key);
  }

  public String getFileMetaData(String key) {
    return fileMetaData.get(key);
  }


  public void putFileMetaData(String key, String value) {
    if (fileMetaData == null) {
      throw new IllegalStateException("cannot add file metadata after calling close()");
    }

    if (protectedMetaData.containsKey(key)) {
      throw new IllegalArgumentException("cannot override protected file metadata with key '" + key + "'");
    }

    fileMetaData.put(key, value);
  }

  public void putAllFileMetaData(Map<String,String> metaData) {
    for (Map.Entry<String,String> entry : metaData.entrySet()) {
      putFileMetaData(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void close() throws IOException {
    try {
      writer.close(fileMetaData);
      fileMetaData = null;
      protectedMetaData = null;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
