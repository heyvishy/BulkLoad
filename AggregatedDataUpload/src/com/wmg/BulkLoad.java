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

import org.apache.log4j.Logger;

public class BulkLoad {

		public static Logger logger = Logger.getLogger(BulkLoad.class);
		
		public static void updateConfigValue(String suffixValue){
			Process cmd;
			Runtime re = Runtime.getRuntime();
			BufferedReader output = null;
			String resultOutput="";
			try
			{ 
			  cmd = re.exec("java -jar cassa.jar write "+suffixValue); 
			  output =  new BufferedReader(new InputStreamReader(cmd.getInputStream()));
			  resultOutput = output.readLine();
			  System.out.println("updateConfigValue Output -->"+resultOutput);
			} 
			catch (IOException e){
			  e.printStackTrace();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public static String readConfigValue(String suffixkey){
			String suffixValue="";
			Process cmd;
			Runtime re = Runtime.getRuntime();
			BufferedReader output = null;
			String resultOutput="";
			try
			{ 
			  cmd = re.exec("java -jar cassa.jar read "+suffixkey);
			  output =  new BufferedReader(new InputStreamReader(cmd.getInputStream()));
			  resultOutput = output.readLine();
			  //below hack is to skip all output lines (e.g logger statements) until it find the text '_1 or _2'
	          while ((resultOutput = output.readLine()) != null) {
	              suffixValue = resultOutput;
	         }
			} 
			catch (IOException e){
			  e.printStackTrace();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			finally{
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return suffixValue;
		}


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
        public static List<String> getHDFSFilePaths(String hdfsDirectory){

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

        
        //executes  for e.g--> hadoop jar /usr/local/code/musicmetric/BulkLoader.jar /user/VishalS/facebook-data.tsv 10.70.99.144 MusicMetricData CF_100;
        public static void doBulkLoad(String jobUser,String jarLocation,String cassandraHost,String keyspace,String cfName,String fullFilePath){
        		
        	Process process;
        		try
                {
                    String[] params =  new String[10];
                    params[0] = "sudo";
                    params[1] = "-u";
                    params[2] = jobUser;
                    params[3] = "hadoop";
                    params[4] = "jar";
                    params[5]= jarLocation;
                    params[6] = fullFilePath;
                    params[7] = cassandraHost;
                    params[8] = keyspace;
                    params[9] = cfName;

                    logger.info("bulk load command params "+Arrays.toString(params));
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
    	public static HashMap<String, String> loadCFMapping(String suffixValue) {
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
		        	//Appends the suffix-key value to cfName
        			String cfName = prop.getProperty(key)+suffixValue;
        			
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
                long startTime = System.currentTimeMillis();
        		logger.info("Starting BulkLoad Process ..........");

                Properties properties = new Properties();  
            	properties.load(new FileInputStream("BulkLoadJob.properties"));
		        
            	String jobUser = properties.getProperty("jobUser");
            	String jarLocation = properties.getProperty("jarLocation");
		        String keyspace = properties.getProperty("keyspace");
		        String cassandraHost = properties.getProperty("cassandraHost");
                

        		//String currentSuffixKey = CassandraWriter.readConfigValue("suffix-key");
		        
                //String applySuffixValue = properties.getProperty("applySuffixValue");
        		String applySuffixValue = properties.getProperty("applySuffixValue")==null?"":properties.getProperty("applySuffixValue");

                String currentSuffixKey="";

		        //If this value is present, then it overrides the whole switch mechanism on bulkLoad side, because then we don't use the suffix-key value present in table. 
                if(applySuffixValue.isEmpty()){
                	logger.info("applySuffixValue is blank so reading value from config table");
    		        currentSuffixKey = readConfigValue("suffix-key");
            		logger.info("current suffixKey -> "+currentSuffixKey);
            		if(currentSuffixKey.equals("_1"))
            			applySuffixValue="_2";
            		else if(currentSuffixKey.equals("_2"))
            			applySuffixValue="_1";
                }
		        
                //Program exits if we do not know the suffix value to apply to cfNames
                if(applySuffixValue.isEmpty() && currentSuffixKey.isEmpty()){
                	logger.info("No suffix value to apply. Exiting BulkLoad!!");
                	System.exit(0);
                }
                
                //Step 1 : Load CF Mapping from config file
                HashMap<String,String> dirCFNameMapping = loadCFMapping(applySuffixValue);
                
                //Step 2: Iterate over Mapping and bulkLoad data
        		if(dirCFNameMapping.size()>0){
        			Iterator it = dirCFNameMapping.entrySet().iterator();
        			while(it.hasNext()){
        				Entry entry = (Entry) it.next();
        				String dirPath = (String) entry.getKey();
        				String cfName=  (String) entry.getValue();
        				logger.info(dirPath+" --> "+cfName);
        				
        				//Step 3 : Get all files for each Mapped dir
                        List<String> files = getHDFSFilePaths(dirPath);
                        logger.info("No. of files found under HDFS dir "+ dirPath + " : "+files.size());
                        
                        //Step 4 : load data from each file->CF Mapping
                        for(String filePath:files){
                            logger.info("Doing file upload for file "+filePath);
                            doBulkLoad(jobUser,jarLocation,cassandraHost,keyspace,cfName,filePath);
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
                //update the suffixValue in MUSICMETRIC_CONFIG value to applySuffixValue
                //CassandraWriter.writeConfigValue(applySuffixValue);
                logger.info("Updating suffix-key value with -->"+applySuffixValue);
                updateConfigValue(applySuffixValue);
                logger.info("Updated the MUSICMETRIC_CONFIG table");

                
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
