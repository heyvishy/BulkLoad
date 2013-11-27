package com.wmg;

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
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraDelete {

	public static Logger logger = Logger.getLogger(CassandraDelete.class);
	
	private static  com.netflix.astyanax.Keyspace keyspace;
	private static AstyanaxContext<Keyspace> keyspaceContext;
	private static List<String> cfNames = new ArrayList<String>();

	public static void delete() {
		
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
	    	.setCqlVersion("3.0.0")
	    	.setTargetCassandraVersion("1.2")
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
/*		ColumnFamily<String, String> CF_INFO =
				  new ColumnFamily<String, String>(
				    //"MUSICMETRIC_CONFIG",              // Column Family Name
					configTableName,
					StringSerializer.get(),   // Key Serializer
				    StringSerializer.get());  // Column Serializer
*/		
		
		//OperationResult<ColumnList<String>> result;
		
		OperationResult<CqlResult<String, String>> result;
		try {

/*			result = keyspace.prepareQuery(CF_INFO).getKey(suffixKey).execute();
			ColumnList<String> columns = result.getResult();
			// Lookup columns in response by name 
			suffixValue = columns.getColumnByName("value").getStringValue();
*/			
			ColumnFamily<String, String> CF_INFO = ColumnFamily.newColumnFamily(configTableName,StringSerializer.get(),StringSerializer.get());
			
			result = keyspace
					.prepareQuery(CF_INFO)
			        //.withCql("SELECT value FROM "+configTableName+" limit 1;")
					.withCql("SELECT value FROM \"MUSICMETRIC_CONFIG\" limit 1;")
			        .execute();
	
			//System.out.println("suffix keyvalue = "+result);
			//below will have 1 row only
			
			String applySuffixValue="";
			
			//String dpSuffixValue = null;
			for (Row<String, String> row : result.getResult().getRows()) {
				ColumnList<String> columns = row.getColumns();
				System.out.println("   suffix keyvalue = : " + columns.getStringValue ("value", null));
				applySuffixValue = columns.getStringValue ("value", null);
			}
			System.out.println("applySuffixValue = "+applySuffixValue);
			
			
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
	        
	        
	        for(String tableName:cfNames){
	        	ColumnFamily<String, String> CF_DELETE = ColumnFamily.newColumnFamily(tableName,StringSerializer.get(),StringSerializer.get());
	        	OperationResult<CqlResult<String, String>> deleteResult;
	        	
	        	deleteResult = keyspace
		                .prepareQuery(CF_DELETE)
		                .withCql("truncate "+ tableName + ";")
		                .execute();	        	
	        }

	        
	        
/*	        for(String tableName:cfNames){
				logger.info("deleting CF :"+tableName);
				keyspace.truncateColumnFamily(tableName);
				logger.info("deleted CF :"+tableName);
	        }
*/

		} 
		catch (ConnectionException e) {
			e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	

	public static void main(String args[]){
		delete();
	}
	

}
