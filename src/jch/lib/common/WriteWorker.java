package jch.lib.common;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class WriteWorker implements Runnable {
	private BufferedWriter writer = null;
	private String fileLocation = "";
	
	public WriteWorker(String fileLocation) {
		

		try {
			this.writer = new BufferedWriter(new FileWriter(fileLocation));
			this.fileLocation = fileLocation;
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

}
