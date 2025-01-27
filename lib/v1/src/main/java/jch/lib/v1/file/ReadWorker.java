package jch.lib.file;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
import java.io.IOException;
//import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
//import java.nio.charset.StandardCharsets;


/**
 * So far, only good for reading file and printing in hex... I know.
 *  
 * @author James Chad Harrison
 * @version 0.0.0
 */
public class ReadWorker implements Runnable {
	private int printWidth = 16;  			//number of bytes
	static private int hexBlockWidthMinor = 8;
	static private int hexBlockWidthMajor = 32;
	static  int hexBlockSpaceWidth = 1;
	
	@SuppressWarnings("unused")
	private int bufferSize = 1024;
	private ByteBuffer currentBuffer;
	private String unknownChar = "."; 				//character to print in place of non-ASCII character
	private RandomAccessFile reader = null;
	private FileChannel channel;
	private long currentOffset = 0;
	private long prevOffset = 0;
	
	public ReadWorker(String newFileLocation) throws IOException {
	    reader = new RandomAccessFile(newFileLocation, "r"); //what does the 'r' stand for again?
	    channel = reader.getChannel();
	}
	
	
	@SuppressWarnings("static-access")
	public int readBytes() throws IOException {
		return readBytes(this.hexBlockWidthMajor);
	}
	
	public int readBytes(int readSize) throws IOException {
		int bytesRead = 0;
		prevOffset = currentOffset;
		if (channel.position() + printWidth > reader.length()) {
			readSize = (int) (reader.length() - channel.position());
		}
	
		currentBuffer = ByteBuffer.allocate(readSize);
		bytesRead = channel.read(currentBuffer);
		currentOffset = bytesRead + currentOffset;
		//buff.flip();
		return bytesRead;
	}
	
	public String currentBufferToString() {
		StringBuilder outputString = new StringBuilder("");
		currentBuffer.rewind();
		int size = currentBuffer.remaining();

		for(int i = 0; i < size ; i++) {
			int cPos = (int)currentBuffer.position(i).get();
			//ASCII printable
			if(cPos >= 32 && cPos < 127) {
				outputString.append(String.valueOf((char)cPos));
			}
			else outputString.append(unknownChar);
		}		
		return outputString.toString();
	}
	
	public String currentBufferToHexString() {
		StringBuilder outputString = new StringBuilder("");
		currentBuffer.rewind();
		int size = currentBuffer.remaining();
		
		for(int i = 0; i < size ; i++) {
			outputString.append(String.format("%02X",currentBuffer.position(i).get()));
			if(i + 1 != size) {
				for(int j = 0; j < hexBlockSpaceWidth; j++)
					outputString.append(" ");
			}
			if(i + 1 != size && (i + 1) % hexBlockWidthMinor == 0) {
				outputString.append(" ");
			}
		}	
		
		return outputString.toString();
	}
	
	public long getCurrentOffset() {
		return this.currentOffset;
	}
	
	public long getPreviousOffset() {
		return this.getPreviousOffset();
	}
	
	public String currentOffsetToHexString() {
		StringBuilder outputString = new StringBuilder("");
		outputString.append(String.format("%08X",this.currentOffset));
		return outputString.toString();
	}
	
	public String previousOffsetToHexString() {
		StringBuilder outputString = new StringBuilder("");
		outputString.append(String.format("%08X",this.prevOffset));
		return outputString.toString();
	}
	
	
	//F1 - Format 1
	public String prettyPrintString_F1() {
		StringBuilder outputString = new StringBuilder("");
		outputString.append(previousOffsetToHexString());
		outputString.append("\t");
		outputString.append(currentBufferToHexString());
		outputString.append("\t");
		outputString.append(currentBufferToString());
		
		return outputString.toString();		
	}
	
	public void close() throws IOException {
		channel.close();
	    reader.close();
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
	
	

}
