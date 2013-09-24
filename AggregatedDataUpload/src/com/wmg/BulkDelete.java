package com.wmg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

public class BulkDelete {

	public static Logger logger = Logger.getLogger(BulkDelete.class);
	
	public static void deleteTables(){
		Process cmd;
		Runtime re = Runtime.getRuntime();
		BufferedReader output = null;
		String resultOutput="";
		try
		{ 
		  cmd = re.exec("java -jar cassa.jar delete all"); 
		  output =  new BufferedReader(new InputStreamReader(cmd.getInputStream()));
		  resultOutput = output.readLine();
		  System.out.println("resultOutput  -->"+resultOutput);
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
	
	public static void main(String[] args) {
		logger.info("Deleting all tables");
		deleteTables();
		logger.info("DONE.");
	}

}
