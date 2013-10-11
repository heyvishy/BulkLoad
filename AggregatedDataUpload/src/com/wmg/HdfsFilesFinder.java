package com.wmg;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsFilesFinder{
	
	public static Logger logger = Logger.getLogger(HdfsFilesFinder.class);
	
	public static void main (String [] args) throws Exception{
                try{
                      
                	logger.info("starting....");
                	logger.info("hdfs file path --> "+args[0]);
                	String hdfsPath = args[0];
                	
                	FileSystem fs = FileSystem.get(new Configuration());
                	logger.info("starting....1");
                        
                     // you need to pass in your hdfs path
                        // hdfs://mahat001.wmg.com:8020/data/raw/musicmetrics/lastfm
                        FileStatus[] status = fs.listStatus(new Path(hdfsPath));
                        
                       // int size = 
                        logger.info("starting....2");
                        for (int i=0;i<status.length;i++){
                                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                                String line;
                                line=br.readLine();
                                while (line != null){
                                        System.out.println(line);
                                        line=br.readLine();
                                }
                        }
                        
/*                        FileStatus[] fileStatus = fs.listStatus(dirPath);
                        if(fileStatus != null) {
                            for (FileStatus fs : fileStatus) {
                                String name = fs.getPath().getName();
                                if(fs.isDir()) {
                                    System.out.println("dir --> " + name);
                                    list(dirPath.getName() + "/" + name);
                                } else {
                                    System.out.println("file --> " + name);
                                }
                            }
                        }
*/                        
                        logger.info("starting....3");
                }catch(Exception e){
                		logger.info("File not found");
                }
        }
}