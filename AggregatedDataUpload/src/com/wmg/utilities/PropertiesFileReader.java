package com.wmg.utilities;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;

public class PropertiesFileReader {

	/*      public static File[] finder( String dirName){
    File dir = new File(dirName);

    return dir.listFiles(new FilenameFilter() {
             public boolean accept(File dir, String filename)
                  { return filename.endsWith(".gz"); }
    } );

}
*/
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Properties properties = new Properties();  
		try {  
		        properties.load(new FileInputStream("BulkLoadJob.properties")); 
		        
		        //System.out.println(" jarLocation = "+properties.getProperty("jarLocation"));
		        //System.out.println(" keyspace = "+properties.getProperty("Keyspace"));
		        
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
