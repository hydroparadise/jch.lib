package jch.lib.test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import jch.lib.log.QLog;

public class HashTest{
	
	static int MAX_CONCURRENCY_LEVEL = 10;
	
	
	final static int lb = 32; 
	final static int ub = 127;
	final static int len = 256;
	public static boolean fnd = false;
	
	
	public static void main(String[] args) {
		QLog.filePath = ".logs\\hasher\\";
		QLog.baseFileName = "hasher_result";
		
		if(args == null || args.length == 0) {
			QLog.log("No arguements provided, running self test");
		}
		else {
			int c = 0;
			for(String arg : args) {
				QLog.log("Arg " + c++ + ": " + arg );
				
			}
		}
		

		
		run("9a5700dd7ccb070abbcc5ecf519dc01cb24285372f7b0a36efd8c0a1ede85138");
	}
	
	
	
	public static void run(String find) {
		
		QLog.log("Finding: " + find);
		
		             
		long index = 165405000000L;
		
		ThreadPoolExecutor exe = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_CONCURRENCY_LEVEL);
		while(!fnd) {
			
			long  lower = index;
			index = 10000000 + index;
			long upper = index - 1;
			
			exe.submit(()->	{
				chunk(find, lower, upper );
				
			});
			
			while(exe.getActiveCount() == MAX_CONCURRENCY_LEVEL) {
				System.out.println("waiting....");
				try {TimeUnit.SECONDS.sleep(10);} catch (InterruptedException e) {e.printStackTrace();}
			}
			
		}
		
		exe.shutdown();
		
	    //wait for all tasks to complete before continuing
	    while(exe.getActiveCount() > 0l) {
	    	
	    	//QLog.log("Task Count: " + exe.getActiveCount());
	    	try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				QLog.log(e.getMessage());
			}
	    }
		
	}
	
	public static void chunk(String find, long lower, long upper) {
		
		QLog.log("Chunk range: " + lower + " -> " +upper);
		char[] test = new char[256];
		int m = 0;
		long t = lower;
		while(t <= upper && !fnd) {
			//System.out.print(t + ": ");
			
			long tt = t;
			int mm = 0;
			//System.out.print("\t" + (tt % (ub-lb) + lb));
			test[mm] = (char)(tt % (ub-lb) + lb);
			
			
			while(tt > (ub-lb - 1) && (tt = tt/(ub-lb))>= 0) {
				mm++;

				//System.out.print("\t" + ((tt % (ub-lb)-1) + lb));
				test[mm] = (char)((tt % (ub-lb)-1) + lb);
			}
			
			
			//System.out.print(tt);
			
			String str = new String(test).substring(0,mm+1);
			String sha256 = hash256(str);

			if(t%1000000==0) {
				System.out.println( t + ": " + str + "->" + sha256);
				System.gc();
			}
			
			
			if(sha256.equalsIgnoreCase(find)) {
				QLog.log("Found!!! " + str + "->" + sha256);
				fnd = true;
			}
			
			
			
			str = null;
			sha256 = null;
			
			
			if(fnd) break;
			//try {Thread.sleep(5);} catch (InterruptedException e) {e.printStackTrace();}
			t++;
		}
		
		System.gc();
	}
	
	
    public static String hash256(String data)  {
        MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        md.update(data.getBytes());
        return bytesToHex(md.digest());
    }
    
    public static String bytesToHex(byte[] bytes) {
        StringBuffer result = new StringBuffer();
        for (byte byt : bytes) result.append(Integer.toString((byt & 0xff) + 0x100, 16).substring(1));
        return result.toString();
    }
	
}