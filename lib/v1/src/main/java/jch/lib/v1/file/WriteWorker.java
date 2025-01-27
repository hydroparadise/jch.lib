package jch.lib.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;


import jch.lib.compress.ExecuteCompressGzip;
import jch.lib.log.QLog;


/***
 * 
 * @author ChadHarrison
 *
 */
public class WriteWorker implements Runnable {
	static int MAX_CONCURRENCY_LEVEL = 4;
	
	public WriteWorker(String fileLocation) {
		

		try {
			this.writer = new BufferedWriter(new FileWriter(fileLocation));
			this.fileLocation = fileLocation;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public enum SplitType {
		RECORDCOUNT,
		FILSIZE
	}

	/***
	 * used split large datasets
	 */
	public static void readWriteFileSplit(String filePath, String fileName, SplitType splitType, long splitOn, 
			boolean compress) {
		QLog.log(filePath + fileName);
		String baseName = fileName.split("[.]")[0];
		String extension = fileName.replaceAll(baseName, "");
		QLog.log(baseName + extension);
		
		int fileCnt = 0;
		long fileSize = 0;
		long recCount = 0;
		String newFileName = "";
		
		ThreadPoolExecutor exe = null;
		if(compress) exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
		
		try {

			BufferedReader reader = new BufferedReader(new FileReader(filePath + fileName));
			BufferedWriter writer = null;
			
			//header
			String header = reader.readLine();
			String line = "";
			
			while(line.equalsIgnoreCase("") || !line.equalsIgnoreCase("\r\n") ) {

				if((splitType == SplitType.FILSIZE && fileSize >= splitOn) ||
			       (splitType == SplitType.RECORDCOUNT && recCount >= splitOn)) {
					QLog.log("File Name: " + newFileName);
					QLog.log("Rec Count: " + recCount);
					QLog.log("File Size: " + fileSize);
					QLog.log("----------------------------");
					recCount = 0;
					fileSize = 0;
				}
				
				if((splitType == SplitType.FILSIZE && fileSize == 0) ||
				   (splitType == SplitType.RECORDCOUNT && recCount == 0)) {
					
					if(fileCnt > 0) {
						writer.close();
					
						if(compress) {
							final String fname = filePath + newFileName;
							QLog.log("Compressing " + fname);
							exe.submit(()->	{
									try {ExecuteCompressGzip.compressGzip(fname, fname + ".gz", false);
									} catch (IOException e) {e.printStackTrace();}
							});
						}
					}
					
					fileCnt++;
					
					String sCnt = String.format("%03d", fileCnt); 
					newFileName = baseName + "_" + sCnt + extension;
					writer = new BufferedWriter(new FileWriter(filePath + newFileName));
					writer.append(header);
					fileSize += header.length();
					
				}

				writer.append(line);
				fileSize += line.length();
				recCount ++;
				
				line = reader.readLine();
				if(line == null || line.equalsIgnoreCase("null")) break;
				else line = "\r\n" + line;

			}
			
			QLog.log("File Name: " + newFileName);
			QLog.log("Rec Count: " + recCount);
			QLog.log("File Size: " + fileSize);
			
			writer.close();
			reader.close();
			
			if(compress) {
				final String fname = filePath + newFileName;
				QLog.log("Compressing " + fname);
				exe.submit(()->	{
					try {ExecuteCompressGzip.compressGzip(fname, fname + ".gz", false);
					} catch (IOException e) {e.printStackTrace();}
				});
				exe.shutdown();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	//writer = new BufferedWriter(new FileWriter(loc1 + "read.txt"));
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
	
	@SuppressWarnings("unused")
	private BufferedWriter writer = null;
	@SuppressWarnings("unused")
	private String fileLocation = "";
}
