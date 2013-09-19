package com.wmg;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class BulkLoad {

		public static Logger logger = Logger.getLogger(BulkLoad.class);

        public static String filterfilePath(String hadoopListCommandOutput){
            //-rwxr-x---   3 wmgload  hdevel   82028513 2013-09-13 12:45 /data/raw/musicmetrics/facebook/part-r-00000.gz
        	//-rwxr-x---   3 hiveload hdevel   772625   2013-09-16 12:42 /data/raw/musicmetrics/amazon_sales/amazon_sales_data.tsv.gz
            String filterfilePath="";   
        	try{
        		String[] splitArray = hadoopListCommandOutput.split("\\s+");
                logger.info("splitArray printed "+Arrays.toString(splitArray));
                logger.info("fullFilePath --> "+splitArray[7]);
                filterfilePath = splitArray[7];
        	}
        	catch(Exception e){
        		logger.error("exception in filterfilePath"+e.toString());
        		e.printStackTrace();
        		filterfilePath="";
        	}
			return filterfilePath;
        }

        //   e.g  hdfsDirectory = '/data/raw/musicmetrics/facebook'
        public static List<String> getFilePaths(String hdfsDirectory){

        	   Process process ;
               List<String> filePaths = new ArrayList<String>();
               try
                {
                    String[] params =  new String[7];
                    params[0] = "sudo";
                    params[1] = "-u";
                    params[2] = "hdfs";
                    params[3] = "hadoop";
                    params[4] = "fs";
                    params[5] = "-ls";
                    params[6] = hdfsDirectory+"/*.gz";

                    logger.info("params --> "+Arrays.toString(params));
                    process = new ProcessBuilder(params).start();

                    InputStream is = process.getInputStream();
                    InputStreamReader isr = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(isr);
                    String line;

                    while ((line = br.readLine()) != null) {
                              logger.info(line);
                              //get filePath and add it to list; skip the found 'N' items line
                              if(!line.contains("found") && !line.contains("items"))
                            	  filePaths.add(filterfilePath(line));
                    }
                }

                catch (IOException e) {
                	logger.error("Exception in getFilePaths method "+e.getMessage());
                	e.printStackTrace();
                }
                catch (Exception e) {
                	logger.error("Exception in getFilePaths method "+e.getMessage());
                	e.printStackTrace();
                }

                return filePaths;
        }

        public static void doBulkLoad(String cfName,String fullFilePath){
        	Properties properties = new Properties();      
        	Process process;
                try
                {
                    //sudo -u hdfs hadoop jar /usr/local/code/musicmetric/MMFBData.jar /user/VishalS/facebook-data.tsv MusicMetricData CF_162
    		        properties.load(new FileInputStream("BulkLoadJob.properties"));
    		        String jarLocation = properties.getProperty("jarLocation");
    		        String keyspace = properties.getProperty("Keyspace");
    		        
    		        //logger.info(" jarLocation = "+jarLocation);
    		        //logger.info(" keyspace = "+keyspace);

                    String[] params =  new String[9];
                    params[0] = "sudo";
                    params[1] = "-u";
                    params[2] = "hdfs";
                    params[3] = "hadoop";
                    params[4] = "jar";
                    //params[5] = "/usr/local/code/musicmetric/MMFBData.jar";
                    params[5]= jarLocation;
                    // params[6] = /user/VishalS/facebook-data.tsv
                    params[6] = fullFilePath;
                    //params[7] = "MusicMetricData";
                    params[7] = keyspace;
                    params[8] = cfName;

                    logger.info("Began upload for Column Family --> "+cfName);
                    long startTime = System.currentTimeMillis();

                    process = new ProcessBuilder(params).start();
                    InputStream is = process.getInputStream();
                    InputStreamReader isr = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(isr);
                    String line;

                    while ((line = br.readLine()) != null) {
                              logger.info(line);
                    }

                    long endTime = System.currentTimeMillis();
                    logger.info("Completed upload for cfName "+cfName);
                    logger.info("Upload for cfName "+cfName+" took " + (endTime - startTime) + " milliseconds");
                }

                catch (Exception e) {
                    logger.error("Exception occurred in upload of CF : "+ cfName + " for input file : "+fullFilePath);
                    e.printStackTrace();
                }
        }

        //gets HashMap  of (key:HDFS dir,value: cfname) based on data in config.properties file
    	public static HashMap<String, String> getFilePathCFMapping(String currentSuffixValue) {
    		Properties prop = new Properties();
        	HashMap<String,String> filePathCFMapping  =  new HashMap<String,String>();
        	try 
        	{
        		prop.load(new FileInputStream("CFMapping.properties"));
		        String key; 
		        Enumeration e = prop.propertyNames(); 
		        while (e.hasMoreElements()) 
		        { 
		        	key = (String)e.nextElement(); 
		        	logger.info(key+" "+prop.getProperty(key)); 

		        	String dirName = key;
		        	//Appends the suffix-key value to cfName in order to point to latest CF Name
        			String cfName = prop.getProperty(key)+currentSuffixValue;
        			
        			logger.info("filePath = "+dirName);
        			logger.info("CF Name = "+cfName);
        			
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

        // java -cp ./ com.hadoop.BulkLoad /data/raw/musicmetrics/facebook facebook_1
        public static void main(String[] args) {

        	try{
                logger.info("Starting BulkLoad Process ..........");
                DailyRollingFileAppender appender = null;
    			appender = new DailyRollingFileAppender(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN), "bulkLoad.log", "'.'yyyy-MM-dd");
    			
    			logger.addAppender(appender);

                //this value would be read from MUSICMETRIC_CONFIG table under keyspace 'MusicMetricData'
                String currentSuffixValue = "";
                
                long startTime = System.currentTimeMillis();

                //Step 1 : Get Mapping from config file
                HashMap<String,String> dirCFNameMapping = getFilePathCFMapping(currentSuffixValue);
                
                //Step 2: Iterate over Mapping
        		if(dirCFNameMapping.size()>0){
        			Iterator it = dirCFNameMapping.entrySet().iterator();
        			while(it.hasNext()){
        				Entry entry = (Entry) it.next();
        				String dirPath = (String) entry.getKey();
        				String cfName=  (String) entry.getValue();
        				logger.info(dirPath+" --> "+cfName);
        				
        				//Step 3 : Get all files for each Mapped dir
                        List<String> files = getFilePaths(dirPath);
                        logger.info("No. of files found under HDFS dir "+ dirPath + " : "+files.size());
                        
                        //Step 4 : load data from each file->CF
                        for(String filePath:files){
                            logger.info("Doing file upload for file "+filePath);
                            doBulkLoad(cfName,filePath);
                            logger.info("Completed  "+filePath);
                        }

        			}
        		}
        		else{
        			logger.info("No data to process !!");
        			logger.info("Ending BulkLoad");
        		}

                
                long endTime = System.currentTimeMillis();
                long millis = endTime - startTime;
                logger.info("Bulk Load Process Finished !!! ");
                logger.info("Overall Upload Process took total " + (endTime - startTime) + " milliseconds");

                String timeUsed = String.format("%d min, %d sec",
                            TimeUnit.MILLISECONDS.toMinutes(millis),
                            TimeUnit.MILLISECONDS.toSeconds(millis) -
                            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(millis))
                        );
                logger.info("In other words time taken : "+timeUsed);
        	}

        	catch(Exception e){
        		logger.error("Exception in main method "+e.toString());
        		e.printStackTrace();
        	}

        }

}
