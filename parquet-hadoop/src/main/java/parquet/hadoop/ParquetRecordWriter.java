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

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.hadoop.api.WriteSupport;
import parquet.schema.MessageType;

/**
 * Writes records to a Parquet file
 *
 * @see ParquetOutputFormat
 *
 * @author Julien Le Dem
 *
 * @param <T> the type of the materialized records
 */
public class ParquetRecordWriter<T> extends RecordWriter<Void, T> {

  private final InternalParquetRecordWriter<T> internalWriter;
  private final MetadataGenerator<T> metadataGenerator;
  private Map<String,String> fileMetaData;
  private Map<String,String> protectedMetaData;

  /**
   *
   * @param w the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param blockSize the size of a block in the file (this will be approximate)
   */
  public ParquetRecordWriter(ParquetFileWriter w, WriteSupport<T> writeSupport,
      MessageType schema,  Map<String, String> extraMetaData,
      MetadataGenerator<T> metadataGenerator, int blockSize,
      int pageSize, BytesCompressor compressor, boolean enableDictionary,
      boolean validating) {
    this .internalWriter = new InternalParquetRecordWriter<T>(w, writeSupport, 
        schema, blockSize, pageSize, compressor, enableDictionary, validating);
    this.metadataGenerator = metadataGenerator;
    this.protectedMetaData = extraMetaData;
    this.fileMetaData = new HashMap<String,String>(protectedMetaData);
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

  /**
   * {@inheritDoc}
   */
  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    if (metadataGenerator != null) {
      metadataGenerator.finalizeFileMetadata(this);
    }

    internalWriter.close(fileMetaData);
    fileMetaData = null;
    protectedMetaData = null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(Void key, T value) throws IOException, InterruptedException {
    if (metadataGenerator != null) {
      metadataGenerator.generateFileMetadata(this, value);
    }

    internalWriter.write(value);
  }

  public static interface MetadataGenerator<T> {
    public void generateFileMetadata(ParquetRecordWriter<T> recordWriter, T value);
    public void finalizeFileMetadata(ParquetRecordWriter<T> recordWriter);
  }
}
