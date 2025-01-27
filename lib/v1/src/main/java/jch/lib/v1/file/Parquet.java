package jch.lib.file;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;



import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import jch.lib.log.QLog;

/****
 * 
Parquet	Transformation	Description
Binary	Binary	1 to 104,857,600 bytes
Binary (UTF8)	String	1 to 104,857,600 characters
Boolean	Integer	-2,147,483,648 to 2,147,483,647
		
		Precision of 10, scale of 0
Double	Double	Precision of 15 digits
Fixed Length Byte Array	Decimal	Decimal value with declared precision and scale. Scale must be less than or equal to precision.
		
		For transformations that support precision up to 38 digits, the precision is 1 to 38 digits, and the scale is 0 to 38.
		
		For transformations that support precision up to 28 digits, the precision is 1 to 28 digits, and the scale is 0 to 28.
		
		If you specify the precision greater than the maximum number of digits, the Data Integration Service converts decimal values to double in high precision mode.
Float	Double	Precision of 15 digits
group (LIST)	Array	Unlimited number of characters.
Int32	Integer	-2,147,483,648 to 2,147,483,647
		
		Precision of 10, scale of 0
Int64	Bigint	-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
		
		Precision of 19, scale of 0
Int64 (TIMESTAMP_MILLIS)	Date/Time	Jan 1, 0001 A.D. to Dec 31, 9999 A.D.
		
		Precision of 29, scale of 9
		
		(precision to the nanosecond)
		
		Combined date/time value.
Int96	Date/Time	Jan 1, 0001 A.D. to Dec 31, 9999 A.D.
		
		Precision of 29, scale of 9
		
		(precision to the nanosecond)
		
		Combined date/time value.
Map	Map	Unlimited number of characters.
Struct	Struct	Unlimited number of characters.
Union	Corresponding primitive data type in a union of ["primitive_type", "null"] or ["null", "primitive_type"].	Dependent on primitive data type

 * @author ChadHarrison
 *
 */
public class Parquet {
	
	private static final Configuration conf = new Configuration();

	public static void readTest2() {
		
		String filePath = "C:\\temp\\t.parquet";
		String dest = "C:\\temp\\t.snappy.parquet";

        try {
        	ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath)).build();
        	
        	
        	for(Field fld :  reader.read().getSchema().getFields()) {
        		QLog.log( fld.name() + " " + fld.schema().getType());
        		try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
        	}
        	
        	
        	/*
			ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new org.apache.hadoop.fs.Path(dest))
			        .withSchema(reader.getSchema())
			        .withConf(new org.apache.hadoop.conf.Configuration())
			        .withCompressionCodec(CompressionCodecName.SNAPPY)
			        .build();
			*/
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				Thread.sleep(2000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}

	}
	
	
	
	public static void readTest() {
		
	    Path file = new Path("C:\\temp\\00.snappy.parquet");
	    Path outUncompressed = new Path("C:\\temp\\ava.uncompressed.parquet");
	    Path outGzipped = new Path("C:\\temp\\data\\java.gzip.parquet");

	    List<Long> readTimes = new ArrayList<Long>();
	    List<Long> writeUTimes = new ArrayList<Long>();
	    List<Long> writeGTimes = new ArrayList<Long>();
	    List<GenericRecord> allRecords = new ArrayList<GenericRecord>();
	    Schema schema = null;

	    for(int i = 0; i < 11; i++) {
	         //read

	    	ParquetReader<GenericRecord> reader;
			try {
				reader = AvroParquetReader.<GenericRecord>builder(file).build();
				
						        GenericRecord record;
		        while((record = reader.read()) != null) {
		        	if(i == 0) {
		              //add once
		               allRecords.add(record);
		               if(schema == null) {
		                  schema = record.getSchema();
		               }
		            }
		         }
		         reader.close();
	
	
		         //write (uncompressed)
		         File t = new File(outUncompressed.toString());
		         t.delete();
	
		         ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outUncompressed)
												            			.withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
												            			.withSchema(schema)
												            			.build();
		         for(GenericRecord wr: allRecords) {
		            writer.write(wr);
		         }
		         writer.close();
	
	
		         writeTest(i, CompressionCodecName.UNCOMPRESSED, writeUTimes, outUncompressed, schema, allRecords);
	
		         writeTest(i, CompressionCodecName.GZIP, writeGTimes, outGzipped, schema, allRecords);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

	      }

	      QLog.log("mean (read): " + avg(readTimes));
	      QLog.log("mean (write uncompressed): " + avg(writeUTimes));
	      QLog.log("mean (write gzip): " + avg(writeGTimes));
		
		
	}
	
	
	@SuppressWarnings("unused")
	private static void writeTest(int iteration, CompressionCodecName codec, List<Long> times,
									Path destPath, Schema schema, List<GenericRecord> records) throws IOException {
		File t = new File(destPath.toString());
		t.delete();
		//TimeWatch timer = TimeWatch.start();
		
		ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(destPath)
															   .withCompressionCodec(codec)
															   .withSchema(schema)
															   .build();
		for(GenericRecord wr: records) {
			writer.write(wr);
		}
		writer.close();

	}
		
	private static Long avg(List<Long> list) {
		long sum = 0;
		for(Long time : list) {
			sum += time;
		}
		
		return sum / list.size();
	}
}