package com.wmg.utilities;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

public class MappingProvider {

	public static void main(String[] args) {
		HashMap<String,String> filePathCFMapping = new HashMap<String,String>();
		filePathCFMapping  = getFilePathCFMapping();
		
		if(filePathCFMapping.size()>0){
			
			Iterator it = filePathCFMapping.entrySet().iterator();
			while(it.hasNext()){
				//Map.Entry pairs = (Map.Entry)it.next();
				Entry entry = (Entry) it.next();
				String filePath = (String) entry.getKey();
				String cfName=  (String) entry.getValue();
				System.out.println(filePath+" --> "+cfName);
				
				//doBulkLoad (filePath,cfName);
			}
		}
		else{
			System.out.println("No data to process !!");
			System.out.println("Ending BulkLoad");
		}
		
 
	}

	public static HashMap<String, String> getFilePathCFMapping() {
		Properties prop = new Properties();
    	HashMap<String,String> filePathCFMapping  =  new HashMap<String,String>();
    	try {
            //load a properties file
    		//prop.load(new FileInputStream("config.properties"));
    		//Object "config.properties";
			
    		BufferedReader br = new BufferedReader(new FileReader("config.properties"));
    		String line;
    		
    		//parses properties file and populates filePathCFMapping hashMmap
    		while ((line = br.readLine()) != null) {
    		   // process the line.
    			System.out.println("line -->"+line);
    			String fileCFMapping[] = line.split("=");
    			System.out.println("filePath = "+fileCFMapping[0]);
    			System.out.println("CF Name = "+fileCFMapping[1]);
    			
    			filePathCFMapping.put(fileCFMapping[0], fileCFMapping[1]);
    		}
    		
    		
            //get the property value and print it out
           // System.out.println(prop.getProperty("database"));
    		//System.out.println(prop.getProperty("dbuser"));
    	//System.out.println(prop.getProperty("dbpassword"));
 
    	} catch (IOException e) {
    		e.printStackTrace();
        }
    	  catch(Exception e){
    		  e.printStackTrace();
    	  }
		return filePathCFMapping;
	}

}
