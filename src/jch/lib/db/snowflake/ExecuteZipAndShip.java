package jch.lib.db.snowflake;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.zip.GZIPOutputStream;


import jch.lib.common.QLog;
import jch.lib.test.JchLib_AzureTest;
import jch.lib.test.JchLib_SnowflakeTest;
import jch.lib.common.compress.ExecuteCompressGzip;


class ExecuteZipAndShip extends Thread {
		/***
		 * Zips and sends to Azure Only
		 * @param filePath
		 * @param sourceFileName
		 * @param targetFileName
		 * @param deleteSource
		 * @param azBlobCredLoc
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
		 * Zips and sends to azure and snowflake consumes from azur
		 * @param sfCredsLoc
		 * @param sfDatabase
		 * @param sfSchema
		 * @param sfTable
		 * @param filePath
		 * @param sourceFileName
		 * @param targetFileName
		 * @param deleteSource
		 * @param azBlobCredLoc
		 * @param azBlobDir
		 * @param sfStage
		 * @param sfForceLoad
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
					
					
					QLog.log("azBlobCredLoc: " + this.azBlobCredLoc);
					QLog.log("filePath: " + this.filePath);
					QLog.log("targetFileName: " + this.targetFileName);
					QLog.log("azBlobDir: " + this.azBlobDir);
					
					//if Azure blob directory hasn't been specified, send to container root
					if(this.azBlobDir != null && this.azBlobDir != "")
						JchLib_AzureTest.putBlobFile(
								this.azBlobCredLoc, 
								this.filePath, 
								this.targetFileName, 
								this.azBlobDir);
					else
						JchLib_AzureTest.putBlobFile(
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
				QLog.log(e.toString(),true);
			}
		}
		

	    /***
	     * 
	     * @param azBlobCredsLoc
	     * @param blobFileName
	     * @param sfCredsLoc
	     * @param sfDatabase
	     * @param sfSchema
	     * @param sfTable
	     * @param sfStage
	     * @param valueLimiterCol
	     */
	    public static void sfCopyFromAzureBlob(String azBlobCredsLoc, String azBlobFileName,  String azBlobDir,
	    		String sfCredsLoc, String sfDatabase, String sfSchema, String sfTable, String sfStage, boolean forceLoad) {
	    	
	    	
	    	
	    	
			String sql = SnowflakeDiscovery.sqlCopyFromStage(azBlobFileName, azBlobDir, sfSchema, sfTable, sfStage, forceLoad);
			
			QLog.log(sql);

			try {
				
				java.sql.Connection sfCn = null;
				sfCn = JchLib_SnowflakeTest.getConnection(sfCredsLoc, sfDatabase, "PUBLIC");
				Statement sfStatement = sfCn.createStatement();
				

				sfStatement.execute(sql);
				sfStatement.close();
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.toString(),true);
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
	
	
	