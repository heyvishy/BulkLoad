package com.vishal.bulkDelete;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

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
import com.netflix.astyanax.model.CqlResult;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class BulkDeleteRecursive {

	public static Logger logger = Logger.getLogger(BulkDeleteRecursive.class);
	
	private static  com.netflix.astyanax.Keyspace keyspace;
	private static AstyanaxContext<Keyspace> keyspaceContext;
	private static List<String> cfNames = new ArrayList<String>();
	
	/**
	 * 
	 * Main method that kicks of the process to deletes all rows from Cassandra column family(i.e table)
	 */
	public static void main(String args[]){
		
		//initialize Cassandra DB connetion
		init();
		
		boolean isDBNotClean = true;
		
		//do clean up, untill DB is cleaned
		while(isDBNotClean){
			deleteAllData(cfNames);
			isDBNotClean = !isDatabaseClean();
		}
		
	}
	
	/**
	 * delete all data from given colum family 
	 * @param cfNames List of ColumnFamily Names
	 */
	public static void deleteAllData(List<String> cfNames){
		//delete all tables in truncateCF.properties; appropriate ones will be deleted based on suffix-key value in configTable
		for(String tableName:cfNames){
			logger.info("deleting CF :"+tableName);
			try {
				keyspace.truncateColumnFamily(tableName);
				logger.info("deleted CF :"+tableName);
			} 
			catch (ConnectionException e) {
				e.printStackTrace();
			}
			catch(Exception ex){
				ex.printStackTrace();
				logger.error(ex.getMessage());
			}
		}
	}
	
	/**
	 * Checks whether DB is clean
	 * @return
	 */
	public static  boolean isDatabaseClean(){
		boolean isClean = true;
		logger.info("Assuming isClean :---> "+isClean);
		
		for(String cfName:cfNames){
			logger.info(cfName+" hasData "+hasRows(cfName));
			if(hasRows(cfName)==true){
				isClean = false;
				break;
			}
		}
		logger.info("Actually isClean :---> "+isClean);
		return isClean;
	}
	
	/**
	 * DB initialization
	 */
	public static void init(){
		Properties properties = new Properties();
		Properties cfNameProperties = new Properties();
		try {
			properties.load(new FileInputStream("cassandraClient.properties"));
			cfNameProperties.load(new FileInputStream("truncateCF.properties"));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		String cassandraHost = properties.getProperty("cassandraHost");
		String clusterName = properties.getProperty("clusterName");
		String keyspaceName = properties.getProperty("keyspace");
		String configTableName = properties.getProperty("configTable");
		String suffixKey = properties.getProperty("suffixKey");
		
		AstyanaxContext<Keyspace> keyspaceContext = new AstyanaxContext.Builder()
	    .forCluster(clusterName)
	    .forKeyspace(keyspaceName)
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setPort(9160)
	        .setMaxConnsPerHost(1)
	        .setConnectTimeout(6000)
	        .setSeeds(cassandraHost+":9160")
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
	    .buildKeyspace(ThriftFamilyFactory.getInstance());

		keyspaceContext.start();
		keyspace = keyspaceContext.getClient();
		
		String suffixValue="";
		String cfName;
		//below code finds current suffix-key value and sets that value in static variable 'suffixValue'
		ColumnFamily<String, String> CF_INFO =
				  new ColumnFamily<String, String>(
					configTableName,
					StringSerializer.get(),   // Key Serializer
				    StringSerializer.get());  // Column Serializer
		
		OperationResult<ColumnList<String>> result;
		try {

			result = keyspace.prepareQuery(CF_INFO).getKey(suffixKey).execute();
			ColumnList<String> columns = result.getResult();

			// Lookup columns in response by name 
			suffixValue = columns.getColumnByName("value").getStringValue();
			
			String applySuffixValue="";
			if(suffixValue.equals("_1")){
				applySuffixValue="_2";
			}else if(suffixValue.equals("_2")){
				applySuffixValue="_1";
			}
	       Enumeration e = cfNameProperties.propertyNames(); 
	       
	       //appending _1 or _2 to cfName , to make sure we delete appropriate tables
	        while (e.hasMoreElements()) { 
		        cfName = (String)e.nextElement(); 
		        //System.out.println(cfName); 
		        cfNames.add((cfName+applySuffixValue).trim());
	        } 

		} 
		catch (ConnectionException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Checks if any rows exists for given Column Family		
	 * @param columnFamilyName
	 * @return
	 */
	public static boolean hasRows(String columnFamilyName){
		boolean hasRows = false;
		ColumnFamily<String, String> CF_INFO =  new ColumnFamily<String, String>(columnFamilyName,StringSerializer.get(),StringSerializer.get());
		
		OperationResult<CqlResult<String, String>> result;
		try {
			 result = keyspace.prepareQuery(CF_INFO).withCql("SELECT * FROM "+columnFamilyName+" limit 1;").execute();
			 hasRows = result.getResult().hasRows();
		} 
		catch (ConnectionException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		return hasRows;
	}
	
	

}
