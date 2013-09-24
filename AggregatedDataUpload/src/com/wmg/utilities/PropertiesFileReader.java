package com.wmg.utilities;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesFileReader {

	public static void main(String[] args) throws FileNotFoundException, IOException {
		Properties properties = new Properties();
		
/*		properties.load(new FileInputStream("properties/BulkLoadJob.properties")); 
		String key = properties.getProperty("serviceURL")==null?"":properties.getProperty("serviceURL");
		
		System.out.println("key -->"+key);
		if(key.isEmpty()){
			System.out.println("key is blank");
		}
*/		
		try {  
		        properties.load(new FileInputStream("properties/BulkLoadJob.properties")); 
		        
		        String key; 
		        Enumeration e = properties.propertyNames(); 
		        while (e.hasMoreElements()) { 
		        key = (String)e.nextElement(); 
		        System.out.println(key+" "+properties.getProperty(key)); 
		        } 
		        
		} 
		catch (IOException e) { 
			e.printStackTrace();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}

}
