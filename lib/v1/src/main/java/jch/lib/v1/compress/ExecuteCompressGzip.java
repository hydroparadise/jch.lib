package jch.lib.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import jch.lib.log.QLog;

/***
 * Asynchronously compresses files in GZIP format
 * @author harrisonc
 *
 */
public class ExecuteCompressGzip extends Thread {
	public ExecuteCompressGzip(String sourceFile, String targetFile, boolean deleteSource) {
		this.sourceFile = sourceFile;
		this.targetFile = targetFile;
		this.deleteSource = deleteSource;
	}
	
	@Override
	public void run() {
		try {
			ExecuteCompressGzip.compressGzip(this.sourceFile, this.targetFile, this.deleteSource);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ExecuteCompressGzip Exception: " + e.toString(),true);
			QLog.log(e,true);
		}
	}
	
    /***
     * Compresses source to output file in GZIP format
     * 
     * @param source: The file and location of extract to compress (ie,"C:\\temp\\EXTRACT.csv","C:\\temp\\EXTRACT.csv.gz") 
     * @param target: The output file name after compression.
     * @deleteSource: Flag to delete source file after compression.
     * @throws IOException
     */
    public static void compressGzip(String sourceName, String targetName, boolean deleteSource) throws IOException {
    	
        Path source = Paths.get(sourceName);
        Path target = Paths.get(targetName);
    	
        try (GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(target.toFile()));
             FileInputStream fis = new FileInputStream(source.toFile())) {

            //copy file
            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                gos.write(buffer, 0, len);
            }
            
            fis.close();
            gos.close();
            
            
            
            if(deleteSource == true) {
	            File delFile = new File(sourceName);
	            delFile.delete();            	
            }

        }

    }
 
	
	boolean deleteSource;
	String targetFile;
	String sourceFile;
}
	