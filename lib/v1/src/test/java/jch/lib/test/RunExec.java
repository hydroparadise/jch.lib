package jch.lib.test;

import java.io.*;
import java.util.Arrays;

public class RunExec {
			
	static String path = "C:\\temp\\h3.exe";
	static String exec = "h3.exe";
	static String convertParam = " latLngToCell";
	static String lat = " --lat 40.689167";
	static String lon = " --lng -74.044444";
	static String res = " -r 10";
	
	public static void runH3() {
		
		

		
		String cmd = path + exec + convertParam + lat + lon + res;
		System.out.println("Running H3: " + cmd);
		
	    try {
	        String line;
	        Process p =  Runtime.getRuntime().exec(cmd);
	        		//new ProcessBuilder("C:\\temp\h3.exe latLngToCell"
					//,"--lat 40.689167","--lng -74.044444","-r 10").start();
	        
	        BufferedReader bri = new BufferedReader
	          (new InputStreamReader(p.getInputStream()));
	        BufferedReader bre = new BufferedReader
	          (new InputStreamReader(p.getErrorStream()));
	        while ((line = bri.readLine()) != null) {
	          System.out.println(line);
	        }
	        bri.close();
	        while ((line = bre.readLine()) != null) {
	          System.out.println(line);
	        }
	        bre.close();
	        p.waitFor();
	        System.out.println("Done.");
	      }
	      catch (Exception err) {
	        err.printStackTrace();
	      }
	}
	
	
	public static void runH3_1() {
		
		System.out.println("Running H3");
		
		Process process = null;
		try {
			process = new ProcessBuilder("C:\\temp\\h3.exe"
					,"latLngToCell","--lat 40.689167","--lng -74.044444","-r 10").start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;

		try {
			while ((line = br.readLine()) != null) {
			  System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}