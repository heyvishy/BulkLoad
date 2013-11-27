package com.wmg;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.wmg.util.OrderedProperties;

public class BulkLoad {

		public static Logger logger = Logger.getLogger(BulkLoad.class);
		private static  com.netflix.astyanax.Keyspace keyspace;
		private static AstyanaxContext<Keyspace> keyspaceContext;
		
		public static void init(){
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream("cassandraClient.properties"));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			catch (Exception e1) {
				e1.printStackTrace();
			}			
			
			String cassandraHost = properties.getProperty("cassandraHost");
			String clusterName = properties.getProperty("clusterName");
			String keyspaceName = properties.getProperty("keyspace");
			
			AstyanaxContext<Keyspace> keyspaceContext = new AstyanaxContext.Builder()
		    .forCluster(clusterName)
		    .forKeyspace(keyspaceName)
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
		        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
		    )
		    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
		        .setPort(9160)
		        .setMaxConnsPerHost(1)
		        .setConnectTimeout(2000)
		        .setSeeds(cassandraHost+":9160")
		    )
		    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());

			keyspaceContext.start();
			keyspace = keyspaceContext.getClient();
		}

		public static String readConfigValue(String suffixkey){
			Properties properties = new Properties();
			String suffixValue="";
			try {
				properties.load(new FileInputStream("cassandraClient.properties"));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			catch (Exception e1) {
				e1.printStackTrace();
			}
			
			String configTableName = properties.getProperty("configTable");
			//MUSICMETRIC_CONFIG
			ColumnFamily<String, String> CF_INFO =
					  new ColumnFamily<String, String>(
					    //"MUSICMETRIC_CONFIG",              // Column Family Name
						configTableName,
						StringSerializer.get(),   // Key Serializer
					    StringSerializer.get());  // Column Serializer
			
			OperationResult<ColumnList<String>> result;
			try {
				result = keyspace.prepareQuery(CF_INFO).getKey(suffixkey).execute();
				ColumnList<String> columns = result.getResult();
				// Lookup columns in response by name 
				suffixValue = columns.getColumnByName("value").getStringValue();
			} 
			catch (ConnectionException e) {
				e.printStackTrace();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			
			return suffixValue;
		}
		
		public static void updateConfigValue(String suffixValue){
			
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream("cassandraClient.properties"));
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			catch (Exception e1) {
				e1.printStackTrace();
			}			
			
			String configTableName = properties.getProperty("configTable");
			String suffixKey = properties.getProperty("suffixKey");
			//MUSICMETRIC_CONFIG
			ColumnFamily<String, String> CF_INFO =
					  new ColumnFamily<String, String>(
					    //"MUSICMETRIC_CONFIG",              // Column Family Name
						configTableName,
						StringSerializer.get(),   // Key Serializer
					    StringSerializer.get());  // Column Serializer
			
			
			OperationResult<ColumnList<String>> result;
			try {
				//write
				keyspace.prepareColumnMutation(CF_INFO, suffixKey, "value").putValue(suffixValue, null).execute();
			}
			catch (ConnectionException e) {
				e.printStackTrace();
			}
			catch(Exception e){
				e.printStackTrace();
			}

		}
		
        /**
         * Finds relative path of files under HDFS dir using Hadoop Interface
         * @param uri
         * @param dirPath
         * @throws IOException
         */
    	private static List<String> getHdfsFiles(String hdfsUriPrefix,String dirPath) throws IOException {
    		String uri = hdfsUriPrefix+dirPath;
    		Configuration conf = new Configuration();
    		FileSystem fs = FileSystem.get(URI.create(uri),conf);
    		
    		Path path = new Path(uri);
    		List<String> filePaths = new ArrayList<String>();
    		
    		FileStatus[] status = fs.listStatus(path);
    		Path[] listedPaths = FileUtil.stat2Paths(status);
    		String filePath="";
    		for(Path p : listedPaths){
    			filePath = dirPath+"/"+p.getName();
    			filePaths.add(filePath);
    		}
    		return filePaths;
    	}
    	
    	/**
    	 * Uploads all files under a given hdfs directory 
    	 * @param jobUser
    	 * @param jarLocation
    	 * @param cassandraHost
    	 * @param keyspace
    	 * @param cfName
    	 * @param dirPath
    	 */
        public static void doHDFSDirBulkLoad(String jobUser,String jarLocation,String cassandraHost,String keyspace,String cfName,String dirPath){
    		
        	String s = null;
        	Process p ; 
        		try
                {
        			long startTime = System.currentTimeMillis();
        			
/*                    String[] params =  new String[10];
                    
                    params[0] = "sudo";
                    params[1] = "-u";
                    params[2] = jobUser;
                    params[3] = "hadoop";
                    params[4] = "jar";
                    params[5]= jarLocation;
                    params[6] = dirPath+"/";
                    params[7] = cassandraHost;
                    params[8] = keyspace;
                    params[9] = cfName;

                    logger.info("doHDFSDirBulkLoad command params "+Arrays.toString(params));
                    

            		StringBuilder commandString = new StringBuilder();
            		for(String str : params) {
            			commandString.append(" ");
            			commandString.append(str);
            		}

                    String command = commandString.toString();
                    logger.info("command about to be executed ---> "+command);
                    p = Runtime.getRuntime().exec(command);
                    BufferedReader stdInput = new BufferedReader(new 
                            InputStreamReader(p.getInputStream()));

                       BufferedReader stdError = new BufferedReader(new 
                            InputStreamReader(p.getErrorStream()));

                       // read the output from the command
                       logger.info("Here is the standard output of the command:\n");
                       while ((s = stdInput.readLine()) != null) {
                    	   logger.info(s);
                       }
                       
                       // read any errors from the attempted command
                       logger.info("Here is the standard error of the command (if any):\n");
                       while ((s = stdError.readLine()) != null) {
                    	   logger.info(s);
                       }
                       
                       stdError.close();
                       stdInput.close();
*/
                  
                   //String bulkLoadDirectoryCommand  = "sudo -u "+jobUser+ " hadoop jar "+jarLocation+" "+dirPath+"/"+" "+cassandraHost+" "+ keyspace+" "+cfName;
        			String bulkLoadDirectoryCommand  = "hadoop jar "+jarLocation+" "+dirPath+"/"+" "+cassandraHost+" "+ keyspace+" "+cfName;
                   logger.info("bulkLoadDirectoryCommand to be executed  "+bulkLoadDirectoryCommand);
                    String[] command = bulkLoadDirectoryCommand.split(" ");
                    p = new ProcessBuilder(command).start();
                    
                    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

                    // read the output from the command
                    logger.info("Here is the standard output of the command:\n");
                    while ((s = stdInput.readLine()) != null) {
                    	   logger.info(s);
                    }
                       
                    // read any errors from the attempted command
                    logger.info("Here is the standard error of the command (if any):\n");
                    while ((s = stdError.readLine()) != null) {
                    		logger.info(s);
                    }
                       
                     stdError.close();
                     stdInput.close();
 
                     // wait for the task complete
                    //p.waitFor();
                    //int ret = p.exitValue();
                    //logger.info("return value -->"+ret);

                    long endTime = System.currentTimeMillis();
                    logger.info("Completed upload for Column Family --> "+cfName);
                    logger.info("Upload for Column Family "+cfName+" took " + (endTime - startTime) + " milliseconds");
                }

                catch (Exception e) {
                    logger.error("Exception occurred in upload of CF : "+ cfName + " for input HDFS dir : "+dirPath);
                    e.printStackTrace();
                }
        }

        //executes  for e.g--> sudo -u wmgload hadoop jar /usr/local/code/musicmetric/BulkLoader.jar /user/VishalS/facebook-data.tsv 10.70.99.144 MusicMetricData Radio_Plays_1;
        public static void doBulkLoad(String jobUser,String jarLocation,String cassandraHost,String keyspace,String cfName,String fullFilePath){
        	String s = null;
        	Process p ;
        	try
                {
        			long startTime = System.currentTimeMillis();
                    //String bulkLoadCommand = "sudo -u "+jobUser+" hadoop jar "+jarLocation+" "+fullFilePath+" "+cassandraHost+" "+keyspace+" "+cfName;
        			String bulkLoadCommand = "hadoop jar "+jarLocation+" "+fullFilePath+" "+cassandraHost+" "+keyspace+" "+cfName;
                    logger.info("bulk load command to be executed  "+bulkLoadCommand);
                    String[] command = bulkLoadCommand.split(" ");
                    p = new ProcessBuilder(command).start();
                    InputStream is = p.getInputStream();
                    InputStreamReader isr = new InputStreamReader(is);
                    BufferedReader br = new BufferedReader(isr);
                    String line;

                    while ((line = br.readLine()) != null) {
                              logger.info(line);
                    }
                    br.close();
                	// wait for the task complete
                    p.waitFor();
                    int ret = p.exitValue();
                    logger.info("return value -->"+ret);
                    
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
    	public static LinkedHashMap<String, String> loadCFMapping(String suffixValue) {
    		OrderedProperties prop = new OrderedProperties();
    		LinkedHashMap<String,String> filePathCFMapping  =  new LinkedHashMap<String,String>();
        	try 
        	{
        		prop.load(new FileInputStream("CFMapping.properties"));
		        String key; 
		        Enumeration e = prop.propertyNames(); 
		        while (e.hasMoreElements()) 
		        { 
		        	key = (String)e.nextElement(); 
		        	String dirName = key;
		        	//Appends the suffix-key value to cfName
        			String cfName = prop.getProperty(key)+suffixValue;
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
    	
    	
    	public static void startBulkLoad() {
    		
        	try{
        		long startTime = System.currentTimeMillis();
        		logger.info("Starting BulkLoad Process ..........");

                Properties properties = new Properties();  
            	properties.load(new FileInputStream("BulkLoadJob.properties"));
		        
            	String jobUser = properties.getProperty("jobUser");
            	String jarLocation = properties.getProperty("jarLocation");
		        String keyspace = properties.getProperty("keyspace");
		        String cassandraHost = properties.getProperty("cassandraHost");
		        String hdfsUriPrefix= properties.getProperty("hdfsUriPrefix");
		        
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
            		
            		logger.info("applySuffixValue  -> "+applySuffixValue);
                }
		        
                //Program exits if we do not know the suffix value to apply to cfNames
                if(applySuffixValue.isEmpty() && currentSuffixKey.isEmpty()){
                	logger.info("No suffix value to apply. Exiting BulkLoad!!");
                	System.exit(0);
                }
                
                //Step 1 : Load CF Mapping from config file
                LinkedHashMap<String,String> dirCFNameMapping = loadCFMapping(applySuffixValue);
                
                //Step 2: Iterate over Mapping and bulkLoad data
        		if(dirCFNameMapping.size()>0){
        			Iterator it = dirCFNameMapping.entrySet().iterator();
        			while(it.hasNext()){
        				Entry entry = (Entry) it.next();
        				String dirPath = (String) entry.getKey();
        				String cfName=  (String) entry.getValue();
        				logger.info(dirPath+" --> "+cfName);
/*
        				//Step 3 : Get all files for each Mapped dir
                        //List<String> files = getHDFSFilePaths(dirPath);
        				List<String> files = getHdfsFiles(hdfsUriPrefix, dirPath);
                        logger.info("No. of files found under HDFS dir "+ dirPath + " : "+files.size());
                        
                        //Step 4 : load data from each file->CF Mapping
                        for(String filePath:files){
                            logger.info("Doing file upload for file "+filePath);
                            doBulkLoad(jobUser,jarLocation,cassandraHost,keyspace,cfName,filePath);
                            logger.info("Completed.");
                        }
*/                        
        				doHDFSDirBulkLoad(jobUser,jarLocation,cassandraHost,keyspace,cfName,dirPath);
        			}
        		}
        		else{
        			logger.info("No data to process !!");
        			logger.info("Ending BulkLoad");
        		}
                
                long endTime = System.currentTimeMillis();
                long millis = endTime - startTime;
                logger.info("Bulk Load Process Finished !!! ");
              
                //logger.info("Updating suffix-key value with -->"+applySuffixValue);
                //updateConfigValue(applySuffixValue);
                //logger.info("Updated the MUSICMETRIC_CONFIG table");

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
    	
        public static void main(String[] args) {
        	//initialize astyanax 
        	init();
        	//start the bulk Load in cassandra
        	startBulkLoad();
        }

}
