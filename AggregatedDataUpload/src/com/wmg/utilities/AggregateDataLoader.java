package com.wmg.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class AggregateDataLoader {

	public static void insertData(String key,String Value){
		
	}
	
	public static void doUploadData(){
	   	System.out.println("inserting data....");
	   	
		try 
		{
			Long startTime = System.currentTimeMillis();

			BufferedReader br = new BufferedReader(new FileReader("facebook-data.txt"));
    		String line;
    		
    		//parses properties file and populates filePathCFMapping hashMmap
    		while ((line = br.readLine()) != null) {
    			//System.out.println("line -->"+line);
    			
    			String keyValuePairs[] = line.split("\\s+");
    			System.out.println("key = "+keyValuePairs[0] + " value = "+keyValuePairs[1]);
    			insertData(keyValuePairs[0],keyValuePairs[1]);
    		}
    		
    		Long endTime = System.currentTimeMillis();
    		System.out.println("Total time taken "+ (startTime-endTime));
    		
    	} catch (IOException e) {
    		e.printStackTrace();
        }
    	  catch(Exception e){
    		  e.printStackTrace();
    	  }
	}
	
	public static void main(String[] args) {
		doUploadData();
 	}

}
