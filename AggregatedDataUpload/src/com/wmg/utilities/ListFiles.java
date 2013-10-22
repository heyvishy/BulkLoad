package com.wmg.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

public class ListFiles {

	public static void main(String[] args) {
		//sudo -u hdfs hadoop fs -ls /data/raw/musicmetrics/facebook/*.gz
		//find /home/VishalS/backup/data/ -type f
		Process process ;
		String dirPath ="/home/VishalS/backup/data/";
		try 
		{
			System.out.println("Finding all files on unix directory "+args[0]);
			
			String[] params =  new String[7];
			params[0] = "find";
			params[1] = "dirPath";
			params[2] = "-type";
			params[3] = "f";
	
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
