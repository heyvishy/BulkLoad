package com.wmg.utilities;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

public class HelloWorld {

	public static Logger logger = Logger.getLogger(HelloWorld.class);
	
	public static void main(String[] args) {
		
//		logger.info("hey everybody. how is it going !!");
		String someString= "vishal";
		
		List<String> content = new ArrayList<String>();
		content.add("vishal");
		content.add("shukla");
		
		System.out.println("content -->"+content);
		
/*		if(StringUtils.isBlank(someString)){
			logger.info("String is blank");
		}
		else{
			logger.info("String is NOT blank");
		}
*/	
	}

}
