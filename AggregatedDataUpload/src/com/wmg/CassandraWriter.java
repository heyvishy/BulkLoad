package com.wmg;

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

public class CassandraWriter {

	public static void main(String args[]){
		AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
	    .forCluster("MUSICMETRIC")
	    .forKeyspace("MusicMetricData")
	    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()      
	        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    )
	    .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
	        .setPort(9160)
	        .setMaxConnsPerHost(1)
	        .setSeeds("10.70.99.144:9160")
	    )
	    .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
	    .buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		Keyspace ks = context.getEntity();
		
		System.out.println("");
		
		//MUSICMETRIC_CONFIG
		ColumnFamily<String, String> CF_INFO =
				  new ColumnFamily<String, String>(
				    "MUSICMETRIC_CONFIG",              // Column Family Name
				    StringSerializer.get(),   // Key Serializer
				    StringSerializer.get());  // Column Serializer
		
		
		OperationResult<ColumnList<String>> result;
		try {
			result = ks.prepareQuery(CF_INFO)
			    .getKey("suffix-key")
			    .execute();
			
			ColumnList<String> columns = result.getResult();

			// Lookup columns in response by name 
			String value        = columns.getColumnByName("value").getStringValue();
		//	long counter   = columns.getColumnByName("loginCount").getLongValue();
		//	String address = columns.getColumnByName("address").getStringValue();

			// Or, iterate through the columns
			for (com.netflix.astyanax.model.Column<String> c : columns) {
			  System.out.println(c.getName());
			}
			
		} catch (ConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
