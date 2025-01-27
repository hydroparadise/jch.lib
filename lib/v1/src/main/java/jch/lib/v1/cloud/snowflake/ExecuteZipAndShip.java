package jch.lib.cloud.snowflake;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;

import jch.lib.cloud.azure.AzureBlob;
import jch.lib.log.QLog;
import jch.lib.compress.ExecuteCompressGzip;

/***
 * Takes a local file and will GZIP and then to send to an Azure Blob conatainer 
 * 
 * @author harrisonc
 * @TODO: Abstract away from Azure specific and make more abstract to other cloud platforms
 */
class ExecuteZipAndShip extends Thread {
		/***
		 * Constructor: Zips and sends to Azure Only
		 * 
		 * @param String filePath:
		 * @param String sourceFileName:
		 * @param String targetFileName:
		 * @param boolean deleteSource:
		 * @param String azBlobCredLoc:
		 */
		public ExecuteZipAndShip(
				String filePath, String sourceFileName, String targetFileName, boolean deleteSource, 
				String azBlobCredLoc, String azBlobDir) {
			this.sourceFileName = sourceFileName;
			this.targetFileName = targetFileName;
			this.deleteSource = deleteSource;
			this.azBlobCredLoc = azBlobCredLoc;
			this.filePath = filePath;
		}
		
		
		/***
		 * Constructor: Zips and sends to azure and Snowflake consumes from Azure
		 * 
		 * @param String sfCredsLoc:
		 * @param String sfDatabase:
		 * @param String sfSchema:
		 * @param String sfTable:
		 * @param String filePath:
		 * @param String sourceFileName:
		 * @param String targetFileName:
		 * @param boolean deleteSource:
		 * @param String azBlobCredLoc:
		 * @param String azBlobDir:
		 * @param String sfStage:
		 * @param boolean sfForceLoad:
		 */
		public ExecuteZipAndShip(
				String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable,  
				String filePath, String sourceFileName, String targetFileName, boolean deleteSource, 
				String azBlobCredLoc, String azBlobDir,	String sfStage, boolean sfForceLoad) {
			
			this.sourceFileName = sourceFileName;
			this.targetFileName = targetFileName;
			this.deleteSource = deleteSource;
			this.azBlobCredLoc = azBlobCredLoc;
			this.filePath = filePath;
			this.sfCredsLoc = sfCredsLoc;
			this.sfDatabase = sfDatabase;
			this.sfSchema = sfSchema;
			this.sfTable = sfTable;
			this.sfForceLoad = sfForceLoad;
			this.sfStage = sfStage;
			this.azBlobDir = azBlobDir;
		
		}
		
		
		@Override
		public void run() {
			String sourceFile = this.filePath + this.sourceFileName;
			String targetFile = this.filePath + this.targetFileName;
			
			try {
				QLog.log("Compressing " + sourceFile);
				
				ExecuteCompressGzip.compressGzip(sourceFile, targetFile, this.deleteSource);
				
				QLog.log(sourceFile + " has been compressed.");
				
				if(this.azBlobCredLoc != null) {
					
					//for debugging
					QLog.log("ExecuteZipAndShip azBlobCredLoc: " + this.azBlobCredLoc);
					QLog.log("ExecuteZipAndShip filePath: " + this.filePath);
					QLog.log("ExecuteZipAndShip targetFileName: " + this.targetFileName);
					QLog.log("ExecuteZipAndShip azBlobDir: " + this.azBlobDir);
					
					//if Azure blob directory hasn't been specified, send to container root
					if(this.azBlobDir != null && this.azBlobDir != "")
						AzureBlob.putBlobFile(
								this.azBlobCredLoc, 
								this.filePath, 
								this.targetFileName, 
								this.azBlobDir);
					else
						AzureBlob.putBlobFile(
								this.azBlobCredLoc, 
								this.filePath, 
								this.targetFileName);
					
					QLog.log(targetFile + " has been sent to azure.");
							
					//performs copy from Azure to Snowflake for a 7
					sfCopyFromAzureBlob(azBlobCredLoc, this.targetFileName, this.azBlobDir,
							sfCredsLoc, sfDatabase, sfSchema, sfTable, sfStage, sfForceLoad);

					QLog.log(this.targetFileName + " has been copied into Snowflake.");
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
			}
		}
		

		/***
		 * Copy from Azure blob stage file into a Snowflake table.
		 * 
		 * @param String azBlobCredsLoc:
		 * @param String azBlobFileName:
		 * @param String azBlobDir:
		 * @param String sfCredsLoc:
		 * @param String sfDatabase:
		 * @param String sfSchema:
		 * @param String sfTable:
		 * @param String sfStage:
		 * @param boolean forceLoad:
		 */
	    public static void sfCopyFromAzureBlob(String azBlobCredsLoc, String azBlobFileName,  String azBlobDir,
	    		String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, String sfStage, boolean forceLoad) {
	    	
			String sql = SnowflakeDiscovery.sqlCopyFromStage(azBlobFileName, azBlobDir, sfSchema, sfTable, sfStage, forceLoad);
			
			QLog.log(sql);

			try {
				
				java.sql.Connection sfCn = null;
				
				//Connection schema context must be set to "PUBLIC" to access Snowflake stage object
				sfCn = SnowflakeCnString.getConnection(sfCredsLoc, sfDatabase, "PUBLIC");
				Statement sfStatement = sfCn.createStatement();
				
				sfStatement.execute(sql);
				sfStatement.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log("ETL Exception: " + e.toString());
				QLog.log(e);
			}
	    }
	    
		boolean deleteSource;
		String targetFileName;
		String sourceFileName;
		String filePath;
		String azBlobCredLoc;
		String sfCredsLoc; 
		String sfDatabase; 
		String sfSchema; 
		String sfTable;
		boolean sfForceLoad;
		String sfStage;
		String azBlobDir;
	}
	
	
	