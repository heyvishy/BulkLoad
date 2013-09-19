package com.wmg.utilities;

import java.io.IOException;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LogsWriterTest {

	public static Logger logger = Logger.getLogger(LogsWriterTest.class);
    
	public static void logSomething(){
		logger.info("some wah ");
	}
	
    public static void main(String args[]){
    	
    	FileAppender appender;
		try 
		{
			appender = new DailyRollingFileAppender(new PatternLayout(PatternLayout.DEFAULT_CONVERSION_PATTERN), "yourfilename.log", "'.'yyyy-MM-dd");
			
			logger.addAppender(appender );
	    	logger.info("some message");
	    	
	    	logSomething();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    	//Appender fileAppender = new FileAppender();
    }
}