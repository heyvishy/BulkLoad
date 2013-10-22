package com.wmg;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.LinkedHashMap;

import org.apache.log4j.Logger;

import com.wmg.util.OrderedProperties;

public class StringTest {
	
	public static Logger logger = Logger.getLogger(StringTest.class);
	
	public static LinkedHashMap<String, String> loadCFMapping(String suffixValue) {
		
		//Properties prop = new Properties();
		OrderedProperties prop = new OrderedProperties();
		
		LinkedHashMap<String,String> filePathCFMapping  =  new LinkedHashMap<String,String>();
    	try 
    	{
    		prop.load(new FileInputStream("CFMapping.properties"));
    		
/*    		for(String key : prop.stringPropertyNames()) {
    			  String value = prop.getProperty(key);
    			  System.out.println(key + " => " + value);
    		}
*/    		
    	     
    	     
	        String key; 
	        Enumeration e = prop.propertyNames(); 
	        while (e.hasMoreElements()) 
	        { 
	        	key = (String)e.nextElement(); 
	        	String dirName = key;
	        	//Appends the suffix-key value to cfName
    			String cfName = prop.getProperty(key)+suffixValue;
    			logger.info(dirName+"  --> "+cfName);
    			filePathCFMapping.put(dirName, cfName);
	        } 
    	} 
    	catch (IOException e) {
    		logger.error("exception in getFilePathCFMapping "+e.getMessage());
    		e.printStackTrace();
        }
    	catch(Exception e){
    		logger.error("exception in getFilePathCFMapping "+e.getMessage());
    		e.printStackTrace();
    	}
		
    	return filePathCFMapping;
	}


	public static void main(String[] args) {
		LinkedHashMap<String,String> hmap = new LinkedHashMap<String,String>();
/*		hmap.put("1", "value1");
		hmap.put("2", "value2");
		hmap.put("3", "value3");
		hmap.put("4", "value4");
		hmap.put("5", "value5");
*/		
		hmap = loadCFMapping("_2");
/*		Iterator it = hmap.entrySet().iterator();
		while(it.hasNext()){
			Entry entry = (Entry) it.next();
			
			System.out.println("key ->"+entry.getKey()+" value -- >"+entry.getValue());
			
		}
*/		
	}

}
