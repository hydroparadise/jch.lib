package jch.lib.test;


import java.io.*;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.ExtractJobConfiguration;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.RetryOption;
import com.opencsv.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.gax.paging.Page;

import com.opencsv.exceptions.CsvValidationException;
import org.threeten.bp.Duration;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.auth.*;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;



import jch.lib.log.QLog;

//TODO: debug "check name" methods
public class AppDataSource {
	AppDataSource(){}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  AWS Stuff
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////	
	
	
	// https://medium.com/@akhouri/aws-credentials-with-java-sdk-61fe454c3fbd
	public static ArrayList<String> awsListS3Files(String bucket, String accessKey, String secretKey, String region) {
		
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		
		AmazonS3 s3 = AmazonS3ClientBuilder
						.standard()
						.withRegion(region)
						.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
						.build();
	
		ObjectListing objs = s3.listObjects(bucket);  //s3.listObjectsV2(bucket);
		
		ArrayList<String> output = new ArrayList<String>();
		for(S3ObjectSummary obj : objs.getObjectSummaries()) {
			output.add(obj.getKey());
			//QLog.log(obj.getKey());
			
		}
		
		while(objs.isTruncated()) {
			//QLog.log("results truncated");
				
			objs = s3.listNextBatchOfObjects(objs);
			for(S3ObjectSummary obj : objs.getObjectSummaries()) {
				output.add(obj.getKey());
				//QLog.log(obj.getKey());
				
			}
		}
		
		
		s3.shutdown();
		return output;
	}
	
	public static void awsListBuckets(String bucket, String accessKey,  String secretKey, String region) {
		
		BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
		
		// https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/creating-clients.html
		//AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
		AmazonS3 s3 = AmazonS3ClientBuilder
						.standard()
						.withRegion(region)
						.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
						.build();
						
		
		//SystemPropertiesCredentialsProvider spcp = new SystemPropertiesCredentialsProvider();
		
		// https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/SystemPropertiesCredentialsProvider.html
		
		List<Bucket> buckets = s3.listBuckets();
		System.out.println("Your {S3} buckets are:");
		for (Bucket b : buckets) {
		    System.out.println("* " + b.getName());
		}
		
	}
	

	public static ArrayList<BigQueryHelper.Field> getKnownTags() {
		//QLog.baseFileName = "CSVRead1";
		//QLog.filePath = "C:\\temp\\";
		
		ArrayList<BigQueryHelper.Field> output = new ArrayList<BigQueryHelper.Field>();
		
		List<List<String>> csvRecords = readCsv("./data/known_tags.csv",",",1);
		
		
		//tag,field,Count
		for(List<String> row : csvRecords) {
			BigQueryHelper.Field fld = new BigQueryHelper.Field(row.get(1), new BigQueryHelper.Tag(row.get(0)));
			output.add(fld);

		}
		
		return output;
	}
	
	
	public static String wrapQualifier(String value) {
		return wrapQualifier(value, "\"", true);
	}
	
	public static String wrapQualifier(String value, String textQualifier, boolean printNull) {
		String output = "";
		if(textQualifier == null) textQualifier = "";
		
		if(value == null || (value != null && value.equalsIgnoreCase("null"))) {
			if(printNull)
				output = "null";
			else output = textQualifier + textQualifier;
		}
		else {
			output = textQualifier + value + textQualifier;
		}
				
		return output;
	}
	
	
	/***
	 * 
	 * Performs simple file name sanitation check and and assembles file name and path to a single string.
	 * will return a new name if file already exists.
	 * 
	 * @param filePath
	 * @param fileName
	 * @return
	 */
	public static String checkFullFileName(String filePath, String fileName) {
		String output = "";
		
		//check path
		String fp = checkFilePath(filePath);
		
		//check filename
		String fn = checkFileNameRename(filePath, fileName);

		//assemble
		output = fp + fn;
		return output;
	}
	
	/***
	 * 
	 * @param filePath
	 * @param fileName
	 * @return
	 */
	public static String checkFileNameRename(String filePath, String fileName) {
		//QLog.log("check name: " + filePath + fileName);
		String output = "out";
		if(filePath != null && filePath.length() > 0) {
			//output = filePath;
			
			String testPath = filePath + fileName;
			int i = 1;
			String reasm = "";
			while(Files.exists(Paths.get(testPath))) {
				i++;
				reasm = "";
				//check for file extension
				//increment by one on file name before extension
				if(fileName.contains(".")) {

					//split file based on ".", provide incremented name, the reassemble
					String split[] = fileName.split("[.]");
					for(int k = 0; k < split.length ; k++) {
						
						//target 2nd to last string
						//	"filename.csv" -> ["filename"],["csv]
						//                       target
						//  "filename" -> "filename_2"
						//	reasm <- "filname_2"
						//  reasm <- "csv"
						if(k == split.length - 2) {
							reasm += split[k] + "_" + i;
						}
						else
							reasm += split[k];
						
						if(k < split.length - 1)
							reasm += ".";
					}
				}
				else {

					testPath = filePath + fileName + "_" + i;
				}
				
				testPath = filePath + reasm;
				//QLog.log(testPath);
			}
			output = reasm;
		}
		return output;
	}
	
	/****
	 * 
	 * @param filePath
	 * @return
	 */
	public static String checkFilePath(String filePath) {
		String output = "";
		final String DEFAULT = "/";  //assume linux or URI format for now
		if(filePath != null) { //&& filePath.length() > 0) {
			output = filePath;
			//QLog.log("checking character position: " + output.charAt(output.length() - 1));
			
			//check beginning
			if(!output.matches("^[a-zA-Z]:.*")) { //checks for drive letter and colon
				//check end
				if(output.charAt(0) != '\\' && 
				   output.charAt(0) != '/' ) {
					
					if(output.contains("\\")) {//windows style
						output = "\\" + output;
					} else
					if(filePath.contains("/")) {//linux or URI style
						output = "/" + output;
					}
					else {
						//what to do?
						
						output = DEFAULT + DEFAULT;
					}
				}
			}
				
			
			//check end
			if(output.charAt(output.length() - 1) != '\\' && 
			   output.charAt(output.length() - 1) != '/' ) {
				
				if(output.contains("\\") || output.matches("^[a-zA-Z]:.*")) {//windows style
					output += "\\";
				} else
				if(filePath.contains("/")) {//linux or URI style
					output += "/";
				}
				else {
					//what to do?
					//assume linux or URI format for now
					output += DEFAULT;
				}
			}
		}
		
		return output;
	}
	

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  CSV Stuff
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


	public static List<List<String>> readCsv(String csvPath, String delim, int skipRows) {
		
		List<List<String>> records = new ArrayList<List<String>>();
		try {
			CSVReader csvReader = new CSVReaderBuilder(new FileReader(csvPath))
					.withSkipLines(skipRows)
					//TODO: find delimiter option
					.build();

		    String[] values = null;
		    while ((values = csvReader.readNext()) != null) {
		        records.add(Arrays.asList(values));
		    }
		} catch (IOException | CsvValidationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		return records;
	}
	
	
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  BigQuery Stuff
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/***
	 * 
	 * @param projectId
	 * @param bucketName
	 * @return
	 */
	public static ArrayList<String> gcpListObjects(String projectId, String bucketName) {
		ArrayList<String> output = new ArrayList<String>();
	    // The ID of your GCP project
	    // String projectId = "your-project-id";

	    // The ID of your GCS bucket
	    // String bucketName = "your-unique-bucket-name";

	    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
	    Page<Blob> blobs = storage.list(bucketName);

	    for (Blob blob : blobs.iterateAll()) {
	    	output.add(blob.getName());
	    }
	    
	    return output;
	}
	
	/***
	 * 
	 * @param projectId
	 * @param bucketName
	 * @param objectName  "path/test.txt"
	 * @param destFilePath
	 */
	public static void gcpDownloadObject(
		String projectId, String bucketName, String objectName, String destFilePath) {
		// The ID of your GCP project
		// String projectId = "your-project-id";

		// The ID of your GCS bucket
		// String bucketName = "your-unique-bucket-name";

		// The ID of your GCS object
		// String objectName = "your-object-name";

		// The path to which the file should be downloaded
		// String destFilePath = "/local/path/to/file.txt";

		Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

		Blob blob = storage.get(BlobId.of(bucketName, objectName));
		blob.downloadTo(Paths.get(destFilePath));

		QLog.log("Downloaded object "	+ objectName + " from bucket name " + bucketName + " to " + destFilePath);
	}
	
	/***
	 * 
	 * @param projectId
	 * @param bucketName
	 * @param objectName
	 * @param srcfilePath
	 */
	public static void gcpUploadObject(
		String projectId, String bucketName, String objectName, String srcfilePath) {
		// The ID of your GCP project
		// String projectId = "your-project-id";

		// The ID of your GCS bucket
		// String bucketName = "your-unique-bucket-name";

		// The ID of your GCS object
		// String objectName = "your-object-name";

		// The path to your file to upload
		// String filePath = "path/to/your/file"

		Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
		BlobId blobId = BlobId.of(bucketName, objectName);
		BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

		// Optional: set a generation-match precondition to avoid potential race
		// conditions and data corruptions. The request returns a 412 error if the
		// preconditions are not met.
		Storage.BlobWriteOption precondition;
		if (storage.get(bucketName, objectName) == null) {
		      // For a target object that does not yet exist, set the DoesNotExist precondition.
		      // This will cause the request to fail if the object is created before the request runs.
			precondition = Storage.BlobWriteOption.doesNotExist();
		} else {
		      // If the destination already exists in your bucket, instead set a generation-match
		      // precondition. This will cause the request to fail if the existing object's generation
		      // changes before the request runs.
		      precondition =
		          Storage.BlobWriteOption.generationMatch(
		              storage.get(bucketName, objectName).getGeneration());
		}
		
		try {
			storage.createFrom(blobInfo, Paths.get(srcfilePath), precondition);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.getMessage());
		}

		QLog.log("File " + srcfilePath + " uploaded to bucket " + bucketName + " as " + objectName);
	}
	
	
	
	public static void bqExecuteSql(BigQuery bqService, String sql) {
		
	      // once, and can be reused for multiple requests.
	      
		try {
			QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql).setAllowLargeResults(true).build();

	      // create a view using query and it will wait to complete job.
			Job job = bqService.create(JobInfo.of(config));
			job = job.waitFor();
			if (job.isDone()) {	
			} else {
				;
			}
	    } catch (BigQueryException | InterruptedException e) {
			System.out.println(e.toString());QLog.log(e.getMessage());	    
	    }
		
	}	
	
	public static void bqExecuteQuery(BigQuery bqService, String sql) {
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql).build();
		
		try {
			bqService.query(queryConfig);
		} catch (JobException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.getMessage());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
		
	}
	
	/***
	 * Returns csv filesize
	 * @param bqService
	 * @param sql
	 * @param csvPath
	 * @param csvFileName
	 * @return
	 */
	public static long bqQueryToCsv(BigQuery bqService, String sql, String localCsvPath, String localCsvFileName, 
			String rowDelim, String colDelim, boolean includeHeader, String textQualifier, boolean printNull) {
		long output = 0;
		if(textQualifier == null) textQualifier = "";
		
		QueryJobConfiguration queryConfig = QueryJobConfiguration
												.newBuilder(sql)
												//.setUseLegacySql(true)
												.setAllowLargeResults(true)
												.build();

		FileWriter file = null;
	
		try {
			file = new FileWriter(localCsvPath + localCsvFileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		BufferedWriter bf = new BufferedWriter(file);
		
		try {
			
			if(includeHeader) {
				//get header info
				String header = "";			
				boolean first = true;
				for(Field fld : bqService.query(queryConfig).getSchema().getFields()) {
					
					//QLog.log(fld.getName() + "\t"+ fld.getType());
					
					if(first) {
						header += wrapQualifier(fld.getName(), textQualifier, printNull); first = false;
					}
					else header += colDelim + wrapQualifier(fld.getName(), textQualifier, printNull);
				}

				QLog.log("Header :" + header);
				bf.write(header);
			}
			
			//print row records
			for(FieldValueList fvl : bqService.query(queryConfig).iterateAll()) {
				
				String row = rowDelim;
				if(!includeHeader) row = rowDelim; 
				boolean first = true;
				for(FieldValue val : fvl) {
					if(first) {
						row += wrapQualifier((String)val.getValue(), textQualifier, false); first = false;
					}
					else row += colDelim + wrapQualifier((String)val.getValue(), textQualifier, printNull);
				}
				
				//QLog.log("Row :" + row);
				bf.write(row);
				
				//log data record as proof data is being processed
				//QLog options are static and should carry over from calling method without having to pass parameters
				output++; if(output%10000==0) QLog.log(output + ": " + row);
			}
			
			bf.close();
			file.close();
		} catch (JobException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log(e.getMessage());
		}
		return output;
	}
	

	/***
	 * 
	 * Caller responsible for sharding through gcsFilName parameter (use wildcard *)
	 * 
	 * https://cloud.google.com/bigquery/docs/exporting-data
	 * 
	 * @param bqService
	 * @param tblProjectId
	 * @param tblDataset
	 * @param tblName
	 * @param gcsBucket
	 * @param gcsPath
	 * @param gcsFileName
	 */
	public static void bqExportTableToGcsGzip(BigQuery bqService,
			String tblProjectId, String tblDataset, String tblName, 
			String gcpBucket, String gcpPath, String gcsFileName,
			String colDelim) {

		
		QLog.log("tblProjectId: " + tblProjectId);		
		QLog.log("tblDataset: " + tblDataset);	
		QLog.log("tblName: " + tblName);	
		QLog.log("gcpBucket: " + gcpBucket);	
		QLog.log("gcpPath: " + padPath(gcpPath));	
		QLog.log("gcsFileName: "+ gcsFileName);	
		QLog.log("colDelim: "+ colDelim);	
		
		
		
		String destinationUri = "gs://" + gcpBucket + padPath(gcpPath) + gcsFileName;
		
		TableId tableId = TableId.of(tblProjectId, tblDataset, tblName);
		QLog.log("tableId: "+ tableId);	
		
		Table table = bqService.getTable(tableId);
		QLog.log("table: "+ table);	

		ExtractJobConfiguration config = ExtractJobConfiguration
											.newBuilder(tableId, destinationUri)
											.setCompression("gzip")
											.setFormat("CSV")
											.setFieldDelimiter(colDelim)
											.build();
		
		String jobName = "jobId_" + UUID.randomUUID().toString();
		JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();
		
		bqService.create(JobInfo.of(jobId, config));
		Job job = bqService.getJob(jobId);
		
		
		try {
			Job completedJob = job.waitFor(
	                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
	                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
			
			if (completedJob == null) {
				QLog.log("Job not executed since it no longer exists.");
			    return;
			} else if (completedJob.getStatus().getError() != null) {
				QLog.log(
			        "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
				return;
			}
			QLog.log("Table export successful. Check in GCS bucket for the CSV file.");
			

			
		} catch (InterruptedException e) {
			QLog.log(e.getMessage());
		}
	}

	/***
	 * 
	 * Caller responsible for sharding through gcsFilName parameter (use wildcard *)
	 * 
	 * https://cloud.google.com/bigquery/docs/exporting-data
	 * 
	 * @param bqService
	 * @param tblProjectId
	 * @param tblDataset
	 * @param tblName
	 * @param gcsBucket
	 * @param gcsPath
	 * @param gcsFileName
	 */
	public static void bqExportTableToGcsCsv(BigQuery bqService,
			String tblProjectId, String tblDataset, String tblName, 
			String gcpBucket, String gcpPath, String gcsFileName,
			String colDelim) {

		
		QLog.log("tblProjectId: " + tblProjectId);		
		QLog.log("tblDataset: " + tblDataset);	
		QLog.log("tblName: " + tblName);	
		QLog.log("gcpBucket: " + gcpBucket);	
		QLog.log("gcpPath: " + padPath(gcpPath));	
		QLog.log("gcsFileName: "+ gcsFileName);	
		QLog.log("colDelim: "+ colDelim);	
		
		
		
		String destinationUri = "gs://" + gcpBucket + padPath(gcpPath) + gcsFileName;
		
		TableId tableId = TableId.of(tblProjectId, tblDataset, tblName);
		QLog.log("tableId: "+ tableId);	
		
		Table table = bqService.getTable(tableId);
		QLog.log("table: "+ table);	

		ExtractJobConfiguration config = ExtractJobConfiguration
											.newBuilder(tableId, destinationUri)
											.setFieldDelimiter(colDelim)
											.build();
		
		String jobName = "jobId_" + UUID.randomUUID().toString();
		JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();
		
		bqService.create(JobInfo.of(jobId, config));
		Job job = bqService.getJob(jobId);
		
		
		try {
			Job completedJob = job.waitFor(
	                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
	                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
			
			if (completedJob == null) {
				QLog.log("Job not executed since it no longer exists.");
			    return;
			} else if (completedJob.getStatus().getError() != null) {
				QLog.log(
			        "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
				return;
			}
			QLog.log("Table export successful. Check in GCS bucket for the CSV file.");
			

			
		} catch (InterruptedException e) {
			QLog.log(e.getMessage());
		}
	}
	

	/***
	 * 
	 * Caller responsible for sharding through gcsFilName parameter (use wildcard *)
	 * 
	 * https://cloud.google.com/bigquery/docs/exporting-data
	 * 
	 * @param bqService
	 * @param tblProjectId
	 * @param tblDataset
	 * @param tblName
	 * @param gcsBucket
	 * @param gcsPath
	 * @param gcsFileName
	 */
	public static void bqExportTableToGcsAvro(BigQuery bqService,
			String tblProjectId, String tblDataset, String tblName, 
			String gcpBucket, String gcpPath, String gcsFileName) {

		
		QLog.log("tblProjectId: " + tblProjectId);		
		QLog.log("tblDataset: " + tblDataset);	
		QLog.log("tblName: " + tblName);	
		QLog.log("gcpBucket: " + gcpBucket);	
		QLog.log("gcpPath: " + padPath(gcpPath));	
		QLog.log("gcsFileName: "+ gcsFileName);	
		
		
		String destinationUri = "gs://" + gcpBucket + padPath(gcpPath) + gcsFileName;
		
		TableId tableId = TableId.of(tblProjectId, tblDataset, tblName);
		QLog.log("tableId: "+ tableId);	
		
		Table table = bqService.getTable(tableId);
		QLog.log("table: "+ table);	

		ExtractJobConfiguration config = ExtractJobConfiguration
											.newBuilder(tableId, destinationUri)
											.setFormat("AVRO")
											.build();
		
		String jobName = "jobId_" + UUID.randomUUID().toString();
		JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();
		
		bqService.create(JobInfo.of(jobId, config));
		Job job = bqService.getJob(jobId);
		
		
		try {
			Job completedJob = job.waitFor(
	                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
	                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
			
			if (completedJob == null) {
				QLog.log("Job not executed since it no longer exists.");
			    return;
			} else if (completedJob.getStatus().getError() != null) {
				QLog.log(
			        "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
				return;
			}
			QLog.log("Table export successful. Check in GCS bucket for the Avro file(s).");
			

			
		} catch (InterruptedException e) {
			QLog.log(e.getMessage());
		}
	}
	
	
	/***
	 * 
	 * Caller responsible for sharding through gcsFilName parameter (use wildcard *)
	 * 
	 * https://cloud.google.com/bigquery/docs/exporting-data
	 * 
	 * @param bqService
	 * @param tblProjectId
	 * @param tblDataset
	 * @param tblName
	 * @param gcsBucket
	 * @param gcsPath
	 * @param gcsFileName
	 */
	public static void bqExportTableToGcsParquet(BigQuery bqService,
			String tblProjectId, String tblDataset, String tblName, 
			String gcpBucket, String gcpPath, String gcsFileName) {

		
		QLog.log("tblProjectId: " + tblProjectId);		
		QLog.log("tblDataset: " + tblDataset);	
		QLog.log("tblName: " + tblName);	
		QLog.log("gcpBucket: " + gcpBucket);	
		QLog.log("gcpPath: " + padPath(gcpPath));	
		QLog.log("gcsFileName: "+ gcsFileName);	
		
		
		String destinationUri = "gs://" + gcpBucket + padPath(gcpPath) + gcsFileName;
		
		TableId tableId = TableId.of(tblProjectId, tblDataset, tblName);
		QLog.log("tableId: "+ tableId);	
		
		Table table = bqService.getTable(tableId);
		QLog.log("table: "+ table);	

		ExtractJobConfiguration config = ExtractJobConfiguration
											.newBuilder(tableId, destinationUri)
											.setFormat("PARQUET")
											.build();
		
		String jobName = "jobId_" + UUID.randomUUID().toString();
		JobId jobId = JobId.newBuilder().setLocation("us").setJob(jobName).build();
		
		bqService.create(JobInfo.of(jobId, config));
		Job job = bqService.getJob(jobId);
		
		
		try {
			Job completedJob = job.waitFor(
	                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
	                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
			
			if (completedJob == null) {
				QLog.log("Job not executed since it no longer exists.");
			    return;
			} else if (completedJob.getStatus().getError() != null) {
				QLog.log(
			        "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
				return;
			}
			QLog.log("Table export successful. Check in GCS bucket for the Parquet file(s).");
			

			
		} catch (InterruptedException e) {
			QLog.log(e.getMessage());
		}
	}
	
	/***
	 * 
	 * Caller responsible for sharding through gcsFilName parameter (use wildcard *)
	 * 
	 * https://cloud.google.com/bigquery/docs/exporting-data
	 * 
	 * @param bqService
	 * @param tblProjectId
	 * @param tblDataset
	 * @param tblName
	 * @param gcsBucket
	 * @param gcsPath
	 * @param gcsFileName
	 */
	public static void bqExportTableToGcsCsv(BigQuery bqService,
			String tblProjectId, String tblDataset, String tblName, 
			String gcpBucket, String gcpPath, String gcsFileName) {

		
		QLog.log("tblProjectId: " + tblProjectId);		
		QLog.log("tblDataset: " + tblDataset);	
		QLog.log("tblName: " + tblName);	
		QLog.log("gcpBucket: " + gcpBucket);	
		QLog.log("gcpPath: " + padPath(gcpPath));	
		QLog.log("gcsFileName: "+ gcsFileName);	
		
		
		
		
		String destinationUri = "gs://" + gcpBucket + padPath(gcpPath) + gcsFileName;
		
		TableId tableId = TableId.of(tblProjectId, tblDataset, tblName);
		QLog.log("tableId: "+ tableId);	
		
		Table table = bqService.getTable(tableId);
		QLog.log("table: "+ table);	
		
		Job job = table.extract("CSV", destinationUri);
		
		try {
			Job completedJob = job.waitFor(
	                  RetryOption.initialRetryDelay(Duration.ofSeconds(1)),
	                  RetryOption.totalTimeout(Duration.ofMinutes(3)));
			
			if (completedJob == null) {
				QLog.log("Job not executed since it no longer exists.");
			    return;
			} else if (completedJob.getStatus().getError() != null) {
				QLog.log(
			        "BigQuery was unable to extract due to an error: \n" + job.getStatus().getError());
				return;
			}
			QLog.log("Table export successful. Check in GCS bucket for the CSV file.");
			

			
		} catch (InterruptedException e) {
		
		}
	}
	
	static String padPath(String gcpPath) {
		String output = gcpPath;		
		if(output.charAt(0) != '/') output = "/" + output;
		if(output.charAt(output.length() - 1) != '/') output = output + "/";
		
		return output;
	}
	
	public static void bqLoadCsvFromGcs(
	    String datasetName, String tableName, String sourceUri, Schema schema) {
			try {
	      // Initialize client that will be used to send requests. This client only needs to be created
	      // once, and can be reused for multiple requests.
				BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

				// Skip header row in the file.
				CsvOptions csvOptions = CsvOptions.newBuilder().setSkipLeadingRows(1).build();

				TableId tableId = TableId.of(datasetName, tableName);
				LoadJobConfiguration loadConfig =
					LoadJobConfiguration.newBuilder(tableId, sourceUri, csvOptions).setSchema(schema).build();

	      // Load data from a GCS CSV file into the table
				Job job = bigquery.create(JobInfo.of(loadConfig));
	      // Blocks until this load table job completes its execution, either failing or succeeding.
				job = job.waitFor();
				if (job.isDone()) {
					System.out.println("CSV from GCS successfully added during load append job");
				} else {
				System.out.println(
	            "BigQuery was unable to load into the table due to an error:"
	                + job.getStatus().getError());
				}
			} catch (BigQueryException | InterruptedException e) {
	    		System.out.println("Column not added during load append \n" + e.toString());
			}
		}
	
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//  SSH/SFTP Stuff
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	/****
	 * Establishes SSH connection with remote host and returns open SSHClient Object
	 * 
	 * @param remoteHost
	 * @param username
	 * @param password
	 * @return
	 * @throws IOException
	 */
	public static SSHClient setupSshj(String remoteHost, String username, String password) throws IOException {
	    SSHClient client = new SSHClient();
	    client.addHostKeyVerifier(new PromiscuousVerifier());
	    client.connect(remoteHost);
	    client.authPassword(username, password);
	    return client;
	}
	
	/***
	 * Uses an existing open SSHClient object to upload a file to remote host
	 * 
	 * @param sshClient
	 * @param localFilePath
	 * @param remoteFilePath
	 * @throws IOException
	 */
	public static void sftpUploadFile(SSHClient sshClient, String localFilePath, String remoteFilePath) throws IOException {
		
	    SFTPClient sftpClient = sshClient.newSFTPClient();
	 
	    sftpClient.put(localFilePath, remoteFilePath);
	 
	    sftpClient.close();
	    sshClient.disconnect();
	}
	
	/***
	 * Uses an existing open SSHClient object download a file from a remote host
	 * 
	 * @param sshClient
	 * @param remoteFilePath
	 * @param localFilePath
	 * @throws IOException
	 */
	public static void sftpDownloadFile(SSHClient sshClient, String remoteFilePath, String localFilePath) throws IOException {
	    SFTPClient sftpClient = sshClient.newSFTPClient();
	 
	    sftpClient.get(remoteFilePath, localFilePath);
	 
	    sftpClient.close();
	    sshClient.disconnect();
	}
	
}