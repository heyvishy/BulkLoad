package com.wmg;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class ListStatus {

	public static void main(String args[]) throws Exception{
		
		String intialUri = "hdfs://mahat001.wmg.com:8020";
		String dirPath = "/data/raw/musicmetrics/lastfm";
		String uri =  intialUri+dirPath;
		System.out.println("finding files for HDFS dirPath "+uri);
		
		getHdfsFiles(uri,dirPath);

	}

	private static void getHdfsFiles(String uri,String dirPath) throws IOException {
		
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
		
		System.out.println("filePaths -->"+filePaths);
	}
	
}
