package jch.lib.file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import jch.lib.file.WriteWorker.SplitType;
import jch.lib.compress.ExecuteCompressGzip;
import jch.lib.log.QLog;

public class FileUtil {
	
	
	public static void removeLine(String filePath, String fileName, String newFilename, long recordNumber) {
		
		FileWriter file = null;
		try {
			file = new FileWriter(filePath + newFilename);
			BufferedWriter writer = new BufferedWriter(file);		
			BufferedReader reader = new BufferedReader(new FileReader(filePath + fileName));					
							
			
			String line = reader.readLine();
			boolean first = true;
			long i = 1;
			while(line != null) {
				
				if(i != recordNumber) {
					if(first) {
						writer.append(line);
						first = false;
					}
					writer.append("\r\n" + line);
				}
				
				line = reader.readLine();
				i++;
				
				if(i == 1 || i == 2 || i%1000000 == 0) 
					QLog.log(i +": "+ line + " " + line.length());
			}
			writer.close();
			reader.close();
		
	
			
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
	 * 
	 * @param shardPath
	 * @param outputPath
	 * @param outputFileName
	 * @param includeHeader
	 * @param splitType
	 * @param splitOn
	 */
	public static void combineShards(String shardPath, String outputPath, String outputFileName, boolean includeHeader, 
										SplitType splitType, long splitOn) {
		ArrayList<String> fileList = listFilesLocal(shardPath);
		
		String baseName = outputFileName.split("[.]")[0];
		String extension = outputFileName.replaceAll(baseName, "");
		
		int fileCnt = 0;
		long fileSize = 0;
		long recCount = 0;		
		String newFileName = "";
		
		String header = null;
		boolean firstRead = true;
		boolean firstWrite = true;
	
		BufferedReader reader = null;
		BufferedWriter writer = null;
		
		try {
			
			for(String shard : fileList) {
				QLog.log(shardPath + shard);
				
				reader = new BufferedReader(new FileReader(shardPath + shard));
				String line = reader.readLine();
				firstRead = true;				
				
				while(line != null) {
					
					//Write limit hit, print
					if((splitType == SplitType.FILSIZE && fileSize >= splitOn) ||
					   (splitType == SplitType.RECORDCOUNT && recCount >= splitOn)) {
								QLog.log("File Name: " + newFileName);
								QLog.log("Rec Count: " + recCount);
								QLog.log("File Size: " + fileSize);
								QLog.log("----------------------------");
								recCount = 0; fileSize = 0;
					}
							
					//Start new file
					if((splitType == SplitType.FILSIZE && fileSize == 0) ||
					   (splitType == SplitType.RECORDCOUNT && recCount == 0)) {
						
						if(fileCnt > 0) {
							writer.close();
						}
						fileCnt++;
						
						String sCnt = String.format("%03d", fileCnt); 
						newFileName = baseName + "_" + sCnt + extension;
						writer = new BufferedWriter(new FileWriter(outputPath + newFileName));
						firstWrite = true;
						
					}
					
					//skip first record if header present
					if(firstRead) {
						if(includeHeader && header == null) header = line;
						if(line != null && header != null && line.equalsIgnoreCase(header))
							line = reader.readLine();
						
						 firstRead = false;
					}					
					
					//omit newline if first record written
					if(firstWrite) {
						if(includeHeader) line = header;
						
						firstWrite = false;
					}
					else line = "\r\n" + line;
					
					//write line
					writer.append(line);	
					//track file size and record count
					fileSize += line.length(); recCount++; 
					
					//get next
					line = reader.readLine();
				}
			}
					
			writer.close();
			reader.close();			
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	/***
	 * 
	 * @param shardPath
	 * @param outputPath
	 * @param outputFileName
	 */
	public static void combineShards(String shardPath, String outputPath, String outputFileName) {
		ArrayList<String> fileList = listFilesLocal(shardPath);
		
		FileWriter file = null;
		
		try {
			file = new FileWriter(outputPath + outputFileName);
			BufferedWriter writer = new BufferedWriter(file);		
			
			boolean firstFile = true;
			for(String fileName : fileList  ) {
				QLog.log(shardPath + fileName);
				
				BufferedReader reader = new BufferedReader(new FileReader(shardPath + fileName));					
							
				boolean firstLine = true;
				String line = reader.readLine();
				while(line != null) {
					//write header from first file
					if(firstFile) {
						writer.append(line + "\r\n");
						firstFile = false;
					}
					//skip header on all other files
					else if(firstLine) {
						line = reader.readLine();
						writer.append(line);
						firstLine = false;
					}
					//print records
					else {
						writer.append("\r\n" + line);
					}
					
					line = reader.readLine();
				}
				
				reader.close();
			}
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	public static boolean renameFilePartial(String filePath, String fileName, String oldVal, String newVal) {
		boolean output;		
		File oldName = new File(filePath + fileName);
		
		String newFileName = fileName.replace(oldVal, newVal);
		File newName = new File(filePath + newFileName);
		
		output = oldName.renameTo(newName);
		
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
		
		//QLog.log("after filepath: " + filePath + " vs " + fp);
		//QLog.log("after fileName: " + fileName + " vs " + fn);
		
		//assemble
		output = fp + fn;
		return output;
	}
	
	
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
	
	
	public static ArrayList<String> listFilesLocal(String filePath) {
		return listFilesLocal(filePath, null);
	}
	
	
	public static ArrayList<String> listFilesLocal(String filePath, String regexFilter) {
		ArrayList<String> output = new ArrayList<String>(); 
		File folder = new File(filePath);
		File[] listOfFiles = folder.listFiles();
		
		if(listOfFiles != null && listOfFiles.length > 0) {
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					
					//String file;
					if(regexFilter != null &&
						(listOfFiles[i].getName().matches(regexFilter) || 
						 listOfFiles[i].getName().contains(regexFilter))) {
						
						output.add(listOfFiles[i].getName());
					}
					else if (regexFilter == null) 
						output.add(listOfFiles[i].getName());
					
				} else if (listOfFiles[i].isDirectory()) {
					//System.out.println("Directory " + listOfFiles[i].getName());
				}
			}
		}
		return output;
	}
	
	
	

	
	public static ArrayList<String> listFoldersLocal(String filePath) {
		return listFoldersLocal(filePath);
	}
	
	public static ArrayList<String> listFoldersLocal(String filePath, String regexFilter) {
		ArrayList<String> output = new ArrayList<String>(); 
		File folder = new File(filePath);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isDirectory()) {
				//System.out.println("File " + listOfFiles[i].getName());
				if(regexFilter != null &&
					(listOfFiles[i].getName().matches(regexFilter) || 
					 listOfFiles[i].getName().contains(regexFilter))) {
					
					output.add(listOfFiles[i].getName());
				}
				else if (regexFilter == null) 
					output.add(listOfFiles[i].getName());
				
			} else if (listOfFiles[i].isDirectory()) {
				//System.out.println("Directory " + listOfFiles[i].getName());
			}
		}
		return output;
	}
}