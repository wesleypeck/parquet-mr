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
package parquet.avro;

import com.google.common.collect.ImmutableList;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import static parquet.filter.ColumnRecordFilter.column;
import static parquet.filter.ColumnPredicates.equalTo;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

/**
 * Other tests exercise the use of Avro Generic, a dynamic data representation. This class focuses
 * on Avro Speific whose schemas are pre-compiled to POJOs with built in SerDe for faster serialization.
 */
public class TestSpecificReadWrite {

  @Test
  public void testReadWriteSpecific() throws IOException {
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false, false);
    ParquetReader<Car> reader = new AvroParquetReader<Car>(path);
    for (int i = 0; i < 10; i++) {
      assertEquals(getVwPolo().toString(),reader.read().toString());
      assertEquals(getVwPassat().toString(),reader.read().toString());
      assertEquals(getBmwMini().toString(),reader.read().toString());
    }
    assertNull(reader.read());
  }

  @Test
  public void testReadWriteSpecificWithDictionary() throws IOException {
    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, true, false);
    ParquetReader<Car> reader = new AvroParquetReader<Car>(path);
    for (int i = 0; i < 10; i++) {
      assertEquals(getVwPolo().toString(),reader.read().toString());
      assertEquals(getVwPassat().toString(),reader.read().toString());
      assertEquals(getBmwMini().toString(),reader.read().toString());
    }
    assertNull(reader.read());
  }

  @Test
  public void testFilterMatchesMultiple() throws IOException {

    Path path = writeCarsToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false, false);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("make", equalTo("Volkswagen")));
    for (int i = 0; i < 10; i++) {
      assertEquals(getVwPolo().toString(),reader.read().toString());
      assertEquals(getVwPassat().toString(),reader.read().toString());
    }
    assertNull( reader.read());
  }

  @Test
  public void testFilterWithDictionary() throws IOException {

    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, true, false);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("make", equalTo("Volkswagen")));
    assertEquals(getVwPolo().toString(),reader.read().toString());
    assertEquals(getVwPassat().toString(),reader.read().toString());
    assertNull( reader.read());
  }

  @Test
  public void testFilterOnSubAttribute() throws IOException {

    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false, false);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(path, column("engine.type", equalTo(EngineType.DIESEL)));
    assertEquals(reader.read().toString(),getVwPassat().toString());
    assertNull( reader.read());

    reader = new AvroParquetReader<Car>(path, column("engine.capacity", equalTo(1.4f)));
    assertEquals(getVwPolo().toString(),reader.read().toString());
    assertNull( reader.read());


    reader = new AvroParquetReader<Car>(path, column("engine.hasTurboCharger", equalTo(true)));
    assertEquals(getBmwMini().toString(),reader.read().toString());
    assertNull( reader.read());
  }

  @Test
  public void testFileMetaData() throws IOException {

    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false, true);

    Configuration conf = new Configuration();
    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, path);
    FileMetaData fileMetaData = metaData.getFileMetaData();
    Map<String,String> keyValueMap = fileMetaData.getKeyValueMetaData();

    assertEquals("putMetaDataValue", keyValueMap.get("putMetaDataKey"));
    assertEquals("putAllMetaDataValue1", keyValueMap.get("putAllMetaDataKey1"));
    assertEquals("putAllMetaDataValue2", keyValueMap.get("putAllMetaDataKey2"));
  }

  public void testProjection() throws IOException {

    Path path = writeCarsToParquetFile(1, CompressionCodecName.UNCOMPRESSED, false, false);
    Configuration conf = new Configuration();

    Schema schema = Car.getClassSchema();
    List<Schema.Field> fields = schema.getFields();

    //Schema.Parser parser = new Schema.Parser();
    List<Schema.Field> projectedFields = new ArrayList<Schema.Field>();
    for (Schema.Field field : fields) {
      String name = field.name();
      if ("optionalExtra".equals(name) ||
          "serviceHistory".equals(name)) {
        continue;
      }

      //Schema schemaClone = parser.parse(field.schema().toString(false));
      Schema.Field fieldClone = new Schema.Field(name, field.schema(), field.doc(), field.defaultValue());
      projectedFields.add(fieldClone);
    }

    Schema projectedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    projectedSchema.setFields(projectedFields);
    AvroReadSupport.setRequestedProjection(conf, projectedSchema);

    ParquetReader<Car> reader = new AvroParquetReader<Car>(conf, path);
    for (Car car = reader.read(); car != null; car = reader.read()) {
      assertEquals(car.getDoors() != null, true);
      assertEquals(car.getEngine() != null, true);
      assertEquals(car.getMake() != null, true);
      assertEquals(car.getModel() != null, true);
      assertEquals(car.getYear() != null, true);
      assertNull(car.getOptionalExtra());
      assertNull(car.getServiceHistory());
    }
  }

  private Path writeCarsToParquetFile( int num, CompressionCodecName compression, boolean enableDictionary, boolean addFileMetaData) throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Car vwPolo   = getVwPolo();
    Car vwPassat = getVwPassat();
    Car bmwMini  = getBmwMini();

    ParquetWriter<Car> writer = new AvroParquetWriter<Car>(path,Car.SCHEMA$, compression,
        DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary);
    for (int i = 0; i < num; i++) {
      writer.write(vwPolo);
      writer.write(vwPassat);
      writer.write(bmwMini);
    }

    if (addFileMetaData) {
      writer.putFileMetaData("putMetaDataKey", "putMetaDataValue");

      Map<String,String> all = new HashMap<String,String>();
      all.put("putAllMetaDataKey1", "putAllMetaDataValue1");
      all.put("putAllMetaDataKey2", "putAllMetaDataValue2");
      writer.putAllFileMetaData(all);
    }

    writer.close();
    return path;
  }

  public static Car getVwPolo() {
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A123 GTR")
        .setMake("Volkswagen")
        .setModel("Polo")
        .setDoors(4)
        .setEngine(Engine.newBuilder().setType(EngineType.PETROL)
                  .setCapacity(1.4f).setHasTurboCharger(false).build())
        .setOptionalExtra(
            Stereo.newBuilder().setMake("Blaupunkt").setSpeakers(4).build())
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder().setDate(1325376000l).setMechanic("Jim").build(),
            Service.newBuilder().setDate(1356998400l).setMechanic("Mike").build()
            ))
        .build();
  }

  public static Car getVwPassat() {
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A123 GXR")
        .setMake("Volkswagen")
        .setModel("Passat")
        .setDoors(5)
        .setEngine(Engine.newBuilder().setType(EngineType.DIESEL)
            .setCapacity(2.0f).setHasTurboCharger(false).build())
        .setOptionalExtra(
            LeatherTrim.newBuilder().setColour("Black").build())
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder().setDate(1325376000l).setMechanic("Jim").build()
        ))
        .build();
  }

  public static Car getBmwMini() {
    return Car.newBuilder()
        .setYear(2010)
        .setRegistration("A124 GSR")
        .setMake("BMW")
        .setModel("Mini")
        .setDoors(4)
        .setEngine(Engine.newBuilder().setType(EngineType.PETROL)
            .setCapacity(1.6f).setHasTurboCharger(true).build())
        .setOptionalExtra(null)
        .setServiceHistory(ImmutableList.of(
            Service.newBuilder().setDate(1356998400l).setMechanic("Mike").build()
        ))
        .build();
  }
}
