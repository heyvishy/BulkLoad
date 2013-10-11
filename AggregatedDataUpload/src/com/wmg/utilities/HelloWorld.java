package com.wmg.utilities;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class HelloWorld {

	public static Logger logger = Logger.getLogger(HelloWorld.class);
	
	public static void main(String[] args) {
		
		logger.info("hey everybody. how is it going !!");
		String someString= "vishal";
		
		if(StringUtils.isBlank(someString)){
			logger.info("String is blank");
		}
		else{
			logger.info("String is NOT blank");
		}
	
	}

}
