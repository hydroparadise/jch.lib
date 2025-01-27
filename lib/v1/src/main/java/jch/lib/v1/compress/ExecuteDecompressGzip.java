package jch.lib.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.*;

import jch.lib.log.QLog;

/***
 * Asynchronously compresses files in GZIP format
 * @author harrisonc
 *
 */
public class ExecuteDecompressGzip extends Thread {
	public ExecuteDecompressGzip(String sourceFile, String targetFile, boolean deleteSource) {
		this.sourceFile = sourceFile;
		this.targetFile = targetFile;
		this.deleteSource = deleteSource;
	}
	
	@Override
	public void run() {
		try {
			ExecuteDecompressGzip.decompressGzip(this.sourceFile, this.targetFile, this.deleteSource);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			QLog.log("ExecuteDecompressGzip Exception: " + e.toString(),true);
			QLog.log(e, true);
		}
	}
	
    /***
     * Decompress source to output file in GZIP format
     * 
     * @param source: The file and location of extract to compress (ie,"C:\\temp\\EXTRACT.csv.gz","C:\\temp\\EXTRACT.csv") 
     * @param target: The output file name after compression.
     * @deleteSource: Flag to delete source file after compression.
     * @throws IOException
     */
    public static void decompressGzip(String sourceName, String targetName, boolean deleteSource) throws IOException {
    	
        Path source = Paths.get(sourceName);
        Path target = Paths.get(targetName);
    	
        try (GZIPInputStream gis =
        		new GZIPInputStream(new FileInputStream(source.toFile()));
        		FileOutputStream fos = new FileOutputStream(target.toFile())) {

            //copy file
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
            
            fos.close();
            gis.close();
            
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
	