package com.wmg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class hadoopListFiles {

	public static void main(String[] args) {
		//sudo -u hdfs hadoop fs -ls /data/raw/musicmetrics/facebook/*.gz
		
		Process process ;
		try 
		{
			System.out.println("Finding all files on HDFS directory "+args[0]);
			
			String[] params =  new String[7];
			params[0] = "sudo";
			params[1] = "-u";
			params[2] = "hdfs";
			params[3] = "hadoop";
			params[4] = "fs";
			params[5] = "-ls";
			params[6] = args[0]+"/*.gz";
	
			System.out.println("params --> "+Arrays.toString(params));
			process = new ProcessBuilder(params).start();
			
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line;
	
			while ((line = br.readLine()) != null) {
				  System.out.println(line);
			}

		} 
		
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

}
