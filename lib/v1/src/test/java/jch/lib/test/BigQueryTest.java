package jch.lib.test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;

import com.amazonaws.services.s3.model.*;
import com.amazonaws.regions.*;
//import com.amazonaws.sdk.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.*;


import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.DatasetListOption;
import com.google.cloud.bigquery.BigQuery.TableListOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.CsvOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics.LoadStatistics;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDataWriteChannel;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.WriteChannelConfiguration;


import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;




import com.google.api.services.bigquery.*;

import jch.lib.cloud.gcp.bigquery.BigQueryDiscovery;
import jch.lib.cloud.gcp.bigquery.BigQueryHelper;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;

import java.io.ByteArrayInputStream;
import java.util.UUID;

//import org.apache.tomcat.util.http.fileupload.FileItem;




/****
 * Used to understand and test the functionality of various 
 * BiqQuery functions and features for development
 * @author ChadHarrison
 *
 */
public class BigQueryTest {
	
	BigQueryTest() {}
	
	
	//https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
    static public String putGcsObject(String bucket, String bucketPath, String googleAccessKeyId, String googleAccessKeySecret, String filePath, String fileName) {

    	String fullFilePath = filePath + fileName;
        String result = "Upload unsuccessfull because ";
        try {

    		// Create a BasicAWSCredentials using Cloud Storage HMAC credentials.
    		BasicAWSCredentials googleCreds =
    		    new BasicAWSCredentials(googleAccessKeyId, googleAccessKeySecret);

    		// Create a new client and do the following:
    		// 1. Change the endpoint URL to use the Google Cloud Storage XML API endpoint.
    		// 2. Use Cloud Storage HMAC Credentials.
    		AmazonS3 s3 =
    		     AmazonS3ClientBuilder.standard()
    		         .withEndpointConfiguration(
    		             new AwsClientBuilder.EndpointConfiguration(
    		                 "https://storage.googleapis.com", "auto"))
    		         .withCredentials(new AWSStaticCredentialsProvider(googleCreds))
    		         .build();

    		File file = new File(fullFilePath);
        	
            S3Object s3Object = new S3Object();

            ObjectMetadata omd = new ObjectMetadata();
            omd.setContentType(Files.probeContentType(file.toPath()));
            omd.setContentLength(file.length());
            omd.setHeader("filename", file.getName());

            ByteArrayInputStream bis = new ByteArrayInputStream(FileUtils.readFileToByteArray(file));
            
            //String keyName  = "Object-"+UUID.randomUUID();
            s3Object.setObjectContent(bis);
            s3.putObject(new PutObjectRequest(bucket, "test_folder/" + fileName, bis, omd));
            s3Object.close();

            result = "Uploaded Successfully.";
        } catch (AmazonServiceException ase) {
           System.out.println("Caught an AmazonServiceException, which means your request made it to Amazon S3, but was "
                + "rejected with an error response for some reason.");

           System.out.println("Error Message:    " + ase.getMessage());
           System.out.println("HTTP Status Code: " + ase.getStatusCode());
           System.out.println("AWS Error Code:   " + ase.getErrorCode());
           System.out.println("Error Type:       " + ase.getErrorType());
           System.out.println("Request ID:       " + ase.getRequestId());

           result = result + ase.getMessage();
        } catch (AmazonClientException ace) {
           System.out.println("Caught an AmazonClientException, which means the client encountered an internal error while "
                + "trying to communicate with S3, such as not being able to access the network.");

           result = result + ace.getMessage();
         }catch (Exception e) {
             result = result + e.getMessage();
       }

        return result;
    }

	
	
	public static void listGcsObjects(String bucket, String googleAccessKeyId, String googleAccessKeySecret) {


		// Create a BasicAWSCredentials using Cloud Storage HMAC credentials.
		BasicAWSCredentials googleCreds =
		    new BasicAWSCredentials(googleAccessKeyId, googleAccessKeySecret);

		// Create a new client and do the following:
		// 1. Change the endpoint URL to use the Google Cloud Storage XML API endpoint.
		// 2. Use Cloud Storage HMAC Credentials.
		AmazonS3 s3 =
		     AmazonS3ClientBuilder.standard()
		         .withEndpointConfiguration(
		             new AwsClientBuilder.EndpointConfiguration(
		                 "https://storage.googleapis.com", "auto"))
		         .withCredentials(new AWSStaticCredentialsProvider(googleCreds))
		         .build();

		ObjectListing objs = s3.listObjects(bucket);  //s3.listObjectsV2(bucket);
			
		ArrayList<String> output = new ArrayList<String>();
		for(S3ObjectSummary obj : objs.getObjectSummaries()) {
			output.add(obj.getKey());
			QLog.log(obj.getKey());
				
		}

			

		 // Explicitly clean up client resources.
		 s3.shutdown();
	}
	
	
	
	/*
    // This example uses RequestBody.fromFile to avoid loading the whole file into memory.
    public static void putS3Object(AmazonS3Client s3, String bucketName, String objectKey, String objectPath) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            
            
   
            
            PutObjectRequest putOb = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .metadata(metadata)
                .build();

            s3.putObject(putOb, RequestBody.fromFile(new File(objectPath)));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);
			
        } catch (AmazonS3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
	*/
	
	public static void testFillRate() {
		QLog.filePath = "C:\\Users\\ChadHarrison\\Desktop\\";
		QLog.baseFileName = "fillrate";
		
		BigQueryHelper.Project proj = new BigQueryHelper.Project("","");
		BigQueryHelper.DataSet dataset = new BigQueryHelper.DataSet(proj, "");
		BigQueryHelper.Table tbl = new BigQueryHelper.Table(dataset, "");
		
		
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		ArrayList<BigQueryHelper.Field> flds = BigQueryHelper.fillFields(tbl, bigquery);
		
		String sql = "";
		
		sql = "WITH C AS ( \n"
			+ "\tSELECT COUNT(*) RecCnt FROM " + tbl.getFullSafeName() + "\n"
			+ "),\n"
			+ "F AS (\n";
		
		for(int i = 1; i <= flds.size(); i++) {
			
			BigQueryHelper.Field fld = flds.get(i-1);
			
			sql = sql + "\tSELECT " + i + " Ordinal, \n" 
			          + "\t\t'" + fld.getName() + "' FieldName, \n";
			
			if(fld.getAbstractDataType().equalsIgnoreCase("TEXT")) {
				sql = sql + "\t\tSUM(CASE WHEN " + fld.getSafeName() + " IS NULL OR " + fld.getSafeName() + " IN ('',' ') THEN 0 ELSE 1 END) FillCount \n";
			}
			else {
				sql = sql + "\t\tSUM(CASE WHEN " + fld.getSafeName() + " IS NULL THEN 0 ELSE 1 END) FillCount \n";
			}
			sql = sql + "\tFROM " + fld.getFullSafeTableName();
			
			if(i != flds.size()) {
				sql = sql + "\n\tUNION ALL\n";
			}
		}
		
		sql = sql + "\n) \n"
				  + "SELECT \n"
				  + "\t F.Ordinal, \n"
				  + "\t F.FieldName, \n"
				  + "\t F.FillCount, \n"
				  + "\t CAST(F.FillCount as FLOAT64) / CAST(C.RecCnt as FLOAT64) FillRate \n"
				  + "FROM F,C \n"
				  + "ORDER BY F.Ordinal";
		
		QLog.log(sql);
	}
	
	
	public static void testDownloadObject() {
		
	}
	
	public static void testListBucket() {
		String projectId = "tes";
		String bucketName = "bkcuet"; 
		
		ArrayList<String> list = AppDataSource.gcpListObjects(projectId, bucketName);
		for(String file : list) {
			QLog.log(file);
		}
	}
	
	public static void testExport() {
		String bucketName = "buck"; 
		String bucketPath = "/download/Run4/"; 
		String baseFilename = "tts;"; 
		String fileExtension = "csv";  
		String format = "CSV";  
		boolean includeHeader = true; 
		String colDelim  = "\\t"; 
		String sql  = "SELECT * FROM `tbl`"; 
		
		String out = BigQueryDiscovery.sqlExportDataShards(
				bucketName, bucketPath, baseFilename, fileExtension, format, includeHeader, colDelim, sql);
		
		QLog.log(out);
		
		BigQueryHelper.Project proj = new BigQueryHelper.Project("du","My First Project");
		
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		AppDataSource.bqExecuteSql(bigquery, sql);
		
	}

	
	
	
	public static void testBigQueryHelper() {
		//BigQueryHelper.test1();
		//BigQueryHelper.test2();
		//BigQueryHelper.test3();
		//BigQueryHelper.test4();
		BigQueryHelper.test6();
		//BigQueryHelper.test7();
		//BigQueryHelper.test8();
		//BigQueryHelper.test9();
		//BigQueryHelper.test10();
	}
	
	
	public static void testSftp() {
		String host = "localhost";
		String user = "ubuntu";
		String password = "qwertyui";
		
		//String localFilePath = "C:\\temp\\data\\known_tags.csv";
		//String remoteFilePath = "known_tags.csv";
		
		String localFilePath = "C:\\temp\\data\\sudo_as_admin_successful.txt";
		String remoteFilePath = ".bash_history";
		
		
		SSHClient sshClient = null;
		
		try {
			sshClient = AppDataSource.setupSshj(host, user, password);
			
			//Works
			//AppDataSource.sftpUploadFile(sshClient, localFilePath, remoteFilePath);
			
			//works
			//SFTPClient sftpClient = sshClient.newSFTPClient();
			//List<RemoteResourceInfo>ls =  sftpClient.ls("/home/ubuntu");
			//for(RemoteResourceInfo rri : ls) {
			//	QLog.log(rri.getPath());
			//}
			
			AppDataSource.sftpDownloadFile(sshClient, remoteFilePath, localFilePath);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	public static void readKnowTags() {
		QLog.baseFileName = "CSVRead2";
		QLog.filePath = "C:\\temp\\";
		
		ArrayList<BigQueryHelper.Field> knownTags = AppDataSource.getKnownTags();
		
		for(BigQueryHelper.Field fld : knownTags) {
			QLog.log(fld.getSafeName() + " - " + fld.getTagName());
		}
	}
	
	public static void fillFieldsSingleTable() {
		QLog.baseFileName = "BQtestFillFields";
		QLog.filePath = "C:\\temp\\";
		
		BigQueryHelper.Project proj = new BigQueryHelper.Project("prj","My First Project");
		BigQueryHelper.DataSet ds = new BigQueryHelper.DataSet(proj, "dataset");
		BigQueryHelper.Table tbl = new BigQueryHelper.Table(ds,"table", new BigQueryHelper.Alias("tbl"));
		
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		ArrayList<BigQueryHelper.Field> flds = BigQueryHelper.fillFields(tbl, bigquery);
		
		for(BigQueryHelper.Field fld : flds) {
			
			if(fld.isDataTypePrimitive())
				QLog.log(fld.getFullSafeName() + " - " + fld.getDataType());
			else {
				QLog.log(fld.getFullSafeName() + " - " + fld.getDataType());
				ArrayList<BigQueryHelper.Field> sflds = fld.expandFieldsFromDataType();
				for(BigQueryHelper.Field sfld : sflds) {
					QLog.log(sfld.getFullSafeName() + " - " + sfld.getDataType());
				}
			}
		}
	}

	
	public static void fillFieldsSuggestTags() {
		//setup logging
		QLog.baseFileName = "BQTestingSuggestFields";
		QLog.filePath = "C:\\temp\\";
		
		ArrayList<BigQueryHelper.Field> fldAtts = null;
		
		try {
			BigQueryHelper.Project proj = new BigQueryHelper.Project(BigQueryOptions.getDefaultProjectId());
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			QLog.log( proj.getSafeName());
			String sqlDataset = BigQueryDiscovery.sqlAllDatasetInformationShema(proj.getFullSafeName());
			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlDataset).build();
			
			//get all available datasets
			ArrayList<BigQueryHelper.DataSet> datasets = new ArrayList<BigQueryHelper.DataSet>(); 
			for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
						
				//TableResult result = queryJob.getQueryResults();
				
				QLog.log(row.get("SCHEMA_NAME").getStringValue());
				datasets.add(new BigQueryHelper.DataSet(proj, BigQueryHelper.extractString(row, "SCHEMA_NAME")));
			}
			
			fldAtts = new ArrayList<BigQueryHelper.Field>();
			
			
			//itterate through all datasets and grab field schema info
			for(BigQueryHelper.DataSet dataset : datasets) {
				String sqlFlds = BigQueryDiscovery.sqlAllColumnInformationShema(dataset.getFullSafeName());
				QLog.log(sqlFlds);
				
				queryConfig = QueryJobConfiguration.newBuilder(sqlFlds).build();
				
				for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
					//System.out.println(row.get("COLUMN_NAME").toString());	
					
					/*
				      + TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, 
				      + DATA_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, IS_HIDDEN, IS_UPDATABLE,
				      + IS_SYSTEM_DEFINED, IS_PARTITIONING_COLUMN, CLUSTERING_ORDINAL_POSITION, COLLATION_NAME,
				      + COLUMN_DEFAULT, ROUNDING_MODE 
					*/
					
					BigQueryHelper.FieldAttributes temp = new BigQueryHelper.FieldAttributes(
								BigQueryHelper.extractString(row, "TABLE_CATALOG"), 
								BigQueryHelper.extractString(row, "TABLE_SCHEMA"), 
								BigQueryHelper.extractString(row, "TABLE_NAME"), 
								BigQueryHelper.extractString(row, "COLUMN_NAME"), 
								BigQueryHelper.extractString(row, "ORDINAL_POSITION"), 
								BigQueryHelper.extractString(row, "IS_NULLABLE"), 
								BigQueryHelper.extractString(row, "DATA_TYPE"), 
								BigQueryHelper.extractString(row, "IS_GENERATED"), 
							    BigQueryHelper.extractString(row, "GENERATION_EXPRESSION"),
							    BigQueryHelper.extractString(row, "IS_STORED"), 
							    BigQueryHelper.extractString(row, "IS_HIDDEN"), 
							    BigQueryHelper.extractString(row, "IS_UPDATABLE"), 
							    BigQueryHelper.extractString(row, "IS_SYSTEM_DEFINED"), 
							    BigQueryHelper.extractString(row, "IS_PARTITIONING_COLUMN"), 
							    BigQueryHelper.extractString(row, "CLUSTERING_ORDINAL_POSITION"), 
							    BigQueryHelper.extractString(row, "COLLATION_NAME"), 
							    BigQueryHelper.extractString(row, "COLUMN_DEFAULT"), 
							    BigQueryHelper.extractString(row, "ROUNDING_MODE"))	;
					
					
					BigQueryHelper.Table tbl = new BigQueryHelper.Table(dataset,temp.getTableName());
					fldAtts.add(new BigQueryHelper.Field(tbl, temp));
				}
			}
		} catch(BigQueryException | JobException | InterruptedException e) {
			e.printStackTrace();
		}	
		
		QLog.log("All Schema info loaded, looking for tag suggestions:");
		
		ArrayList<BigQueryHelper.TagGroup> sugTag = BigQueryHelper.getTestTagSuggestionGroups();
		BigQueryHelper.Tag testTag = null;
		for(BigQueryHelper.Field fa : fldAtts) {
			
			//Primitive field
			if(fa.isDataTypePrimitive()) {
				testTag = BigQueryHelper.TagGroup.suggestTag(fa, sugTag);
				if(testTag != null) {
					QLog.log("Found tag suggestions: " + fa.getFullSafeName() + " - " + testTag.getName());
				}
			}
			//nested fields
			else {
				ArrayList<BigQueryHelper.Field> sfa = fa.expandFieldsFromDataType();
				for(BigQueryHelper.Field sfas : sfa) {
					testTag = BigQueryHelper.TagGroup.suggestTag(sfas, sugTag);
					if(testTag != null) {
						QLog.log("Found nested tag suggestions: " + sfas.getFullSafeName() + " - " + testTag.getName());
					}
				}
				
			}
		}


	}	
	
	
	public static void fillFieldsCountExpandedDataTypes() {
		//setup logging
		QLog.baseFileName = "BQTestingExandNestedFields_adt";
		QLog.filePath = "C:\\temp\\";
		
		try {
			BigQueryHelper.Project proj = new BigQueryHelper.Project(BigQueryOptions.getDefaultProjectId());
			BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
			QLog.log( proj.getSafeName());
			String sqlDataset = BigQueryDiscovery.sqlAllDatasetInformationShema(proj.getFullSafeName());
			
			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sqlDataset).build();
			
			
			ArrayList<BigQueryHelper.DataSet> datasets = new ArrayList<BigQueryHelper.DataSet>(); 
			for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
				QLog.log(row.get("SCHEMA_NAME").getStringValue());
				datasets.add(new BigQueryHelper.DataSet(proj, BigQueryHelper.extractString(row, "SCHEMA_NAME")));
			}
			
			ArrayList<BigQueryHelper.Field> fldAtts = new ArrayList<BigQueryHelper.Field>();
			
			for(BigQueryHelper.DataSet dataset : datasets) {
				String sqlFlds = BigQueryDiscovery.sqlAllColumnInformationShema(dataset.getFullSafeName());
				QLog.log(sqlFlds);
				
				queryConfig = QueryJobConfiguration.newBuilder(sqlFlds).build();
				
				for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
					//System.out.println(row.get("COLUMN_NAME").toString());	
					
					/*
				      + TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, IS_NULLABLE, 
				      + DATA_TYPE, IS_GENERATED, GENERATION_EXPRESSION, IS_STORED, IS_HIDDEN, IS_UPDATABLE,
				      + IS_SYSTEM_DEFINED, IS_PARTITIONING_COLUMN, CLUSTERING_ORDINAL_POSITION, COLLATION_NAME,
				      + COLUMN_DEFAULT, ROUNDING_MODE 
					*/
					
					BigQueryHelper.FieldAttributes temp = new BigQueryHelper.FieldAttributes(
								BigQueryHelper.extractString(row, "TABLE_CATALOG"), 
								BigQueryHelper.extractString(row, "TABLE_SCHEMA"), 
								BigQueryHelper.extractString(row, "TABLE_NAME"), 
								BigQueryHelper.extractString(row, "COLUMN_NAME"), 
								BigQueryHelper.extractString(row, "ORDINAL_POSITION"), 
								BigQueryHelper.extractString(row, "IS_NULLABLE"), 
								BigQueryHelper.extractString(row, "DATA_TYPE"), 
								BigQueryHelper.extractString(row, "IS_GENERATED"), 
							    BigQueryHelper.extractString(row, "GENERATION_EXPRESSION"),
							    BigQueryHelper.extractString(row, "IS_STORED"), 
							    BigQueryHelper.extractString(row, "IS_HIDDEN"), 
							    BigQueryHelper.extractString(row, "IS_UPDATABLE"), 
							    BigQueryHelper.extractString(row, "IS_SYSTEM_DEFINED"), 
							    BigQueryHelper.extractString(row, "IS_PARTITIONING_COLUMN"), 
							    BigQueryHelper.extractString(row, "CLUSTERING_ORDINAL_POSITION"), 
							    BigQueryHelper.extractString(row, "COLLATION_NAME"), 
							    BigQueryHelper.extractString(row, "COLUMN_DEFAULT"), 
							    BigQueryHelper.extractString(row, "ROUNDING_MODE"))	;
					
					
					BigQueryHelper.Table tbl = new BigQueryHelper.Table(dataset,temp.getTableName());
					fldAtts.add(new BigQueryHelper.Field(tbl, temp));
				}
			}
			
			TreeMap<String, Integer> dtCnts = new TreeMap<String, Integer>();
			
			for(BigQueryHelper.Field fa : fldAtts) {
				
				if(fa.isDataTypePrimitive()) {
					if(!dtCnts.containsKey(fa.getDataType()))
						dtCnts.put(fa.getDataType(), 0);
					dtCnts.put(fa.getDataType(),dtCnts.get(fa.getDataType()) +1); 
					
					if(fa.getAbstractDataType().equalsIgnoreCase("OTHER")) {
						QLog.log("Unknown Data Type: " + fa.getFullSafeName() + " " + fa.getDataType() + " " + fa.getAbstractDataType());
					}
				}
				else {
					ArrayList<BigQueryHelper.Field> sfa = fa.expandFieldsFromDataType();
					//QLog.log("Data type expands to " + sfa.size());
					
					for(BigQueryHelper.Field sfas : sfa) {
						if(!dtCnts.containsKey(sfas.getDataType()))
							dtCnts.put(sfas.getDataType(), 0);
						dtCnts.put(sfas.getDataType(),dtCnts.get(sfas.getDataType()) +1); 
						
						if(sfas.getAbstractDataType().equalsIgnoreCase("OTHER")) {
							QLog.log("Unknown Data Type: " + sfas.getFullSafeName() + " " + sfas.getDataType() + " " + sfas.getAbstractDataType());
						}
					}
				}
			}

			
			for(Entry<String, Integer> entry : dtCnts.entrySet()) {
				
				QLog.log(entry.getKey() + ": " + entry.getValue() );
				
			}
			QLog.log(dtCnts.size() + " DataTypes");
			QLog.log(fldAtts.size() + " Records");
		} catch(BigQueryException | JobException | InterruptedException e) {
			
		}
	}	
	
	/***
	 * Lists current default project.  The project will always be associated with 
	 * with the API key.
	 */
	public static void getProjects() {
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		System.out.println( BigQueryOptions.getDefaultProjectId());
		

		//Bigquery.Projects.List;
		//for(Map<String, Object> map : Bigquery.Projects.List) []}
	}
	
	/***
	 * 


	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	public static void runQuerySample() throws InterruptedException {
		 // [START bigquery_query_no_cache]
		 BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		 String query = "SELECT COUNT(*) as Dcount	FROM (SELECT field FROM tabel);";
		 QueryJobConfiguration queryConfig =
		   QueryJobConfiguration.newBuilder(query)
		     // Disable the query cache to force live query evaluation.
		     //.setUseQueryCache(false)
		     .build();
		 
		 // Print the results.
		 System.out.println(query);
		 for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
			 for (FieldValue val : row) {
				 System.out.printf("%s,", val.toString());
			 }
			 System.out.printf("\n");
		 }
	}
	
	public static void runGetDatasetInfo() {
		    // TODO(developer): Replace these variables before running the sample.
		String projectId = "prj";
		String datasetName = "A4";
		getDatasetInfo(projectId, datasetName);
	}


	public static void getDatasetInfo(String projectId, String datasetName) {
		try {
	      	// Initialize client that will be used to send requests. This client only needs to be created
		  	// once, and can be reused for multiple requests.
		  	BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		  	DatasetId datasetId = DatasetId.of(projectId, datasetName);
		  	Dataset dataset = bigquery.getDataset(datasetId);
		
		  	// View dataset properties
		  	String description = dataset.getDescription();
		  	System.out.println(description);
		
		  	// View tables in the dataset
		  	// For more information on listing tables see:
		  	// https://javadoc.io/static/com.google.cloud/google-cloud-bigquery/0.22.0-beta/com/google/cloud/bigquery/BigQuery.html
		  	Page<Table> tables = bigquery.listTables(datasetName, TableListOption.pageSize(100));
		  	
		  	
		  	//tables.iterateAll().forEach(table -> System.out.print(table.getTableId().getTable() + "\n"));
		  	
		  	for(Table table: tables.iterateAll()){
		  		System.out.println(table.getTableId().getTable());
		  		
		  		
		  	}
		  	System.out.println("Dataset info retrieved successfully.");
		} catch (BigQueryException e) {
				System.out.println("Dataset info not retrieved. \n" + e.toString());
		}
	}
	
		
		
	public static Long bqLoadCSV() {
		//https://cloud.google.com/bigquery/docs/batch-loading-data#java
		BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
		String datasetName = null;
		String tableName = null;
		String location = null;
		Path csvPath = null;
		
		TableId tableId = TableId.of(datasetName, tableName);
		WriteChannelConfiguration writeChannelConfiguration =
		    WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.csv()).build();
		
		// The location must be specified; other fields can be auto-detected.
		JobId jobId = JobId.newBuilder().setLocation(location).build();
		TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
		
		// Write data to writer
		try (OutputStream stream = Channels.newOutputStream(writer)) {
		  
			Files.copy(csvPath, stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// Get load job
		Job job = writer.getJob();
		
		try {
			job = job.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LoadStatistics stats = job.getStatistics();
		return stats.getOutputRows();
	}
	
	// Sample to load JSON data from Cloud Storage into a new BigQuery table
	//https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
	public static class LoadJsonFromGCS {
		
		public static void runLoadJsonFromGCS() {
		    // TODO(developer): Replace these variables before running the sample.
		    String datasetName = "MY_DATASET_NAME";
		    String tableName = "MY_TABLE_NAME";
		    String sourceUri = "gs://cloud-samples-data/bigquery/us-states/us-states.json";
		    Schema schema =
		        Schema.of(
		        	Field.newBuilder("name", StandardSQLTypeName.STRING).build(),
			        Field.newBuilder("post_abbr", StandardSQLTypeName.STRING).build()
			    );
		    loadJsonFromGCS(datasetName, tableName, sourceUri, schema);
		}
	
		public static void loadJsonFromGCS(
		      String datasetName, String tableName, String sourceUri, Schema schema) {
			    try {
			      // Initialize client that will be used to send requests. This client only needs to be created
			      // once, and can be reused for multiple requests.
			      BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
		
			      TableId tableId = TableId.of(datasetName, tableName);
			      LoadJobConfiguration loadConfig =
			          LoadJobConfiguration.newBuilder(tableId, sourceUri)
			              .setFormatOptions(FormatOptions.json())
			              .setSchema(schema)
			              .build();
		
			      // Load data from a GCS JSON file into the table
			      Job job = bigquery.create(JobInfo.of(loadConfig));
			      // Blocks until this load table job completes its execution, either failing or succeeding.
			      job = job.waitFor();
			      if (job.isDone()) {
			        System.out.println("Json from GCS successfully loaded in a table");
			      } else {
			        System.out.println(
			            "BigQuery was unable to load into the table due to an error:"
			                + job.getStatus().getError());
			      }
			    } catch (BigQueryException | InterruptedException e) {
			      System.out.println("Column not added during load append \n" + e.toString());
			    }
			  }
		}
		
	
	// Sample to load CSV data from Cloud Storage into a new BigQuery table
	public static class LoadCsvFromGcs {

	  public static void runLoadCsvFromGcs() throws Exception {
	    // TODO(developer): Replace these variables before running the sample.
	    String datasetName = "MY_DATASET_NAME";
	    String tableName = "MY_TABLE_NAME";
	    String sourceUri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv";
	    Schema schema =
	        Schema.of(
	        	Field.newBuilder("name", StandardSQLTypeName.STRING).build(),
	        	Field.newBuilder("post_abbr", StandardSQLTypeName.STRING).build()
	        );
	    loadCsvFromGcs(datasetName, tableName, sourceUri, schema);
	  }
	
	
	public static void loadCsvFromGcs(
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
	}
	

	
	
}