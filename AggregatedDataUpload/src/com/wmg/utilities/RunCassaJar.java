package com.wmg.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class RunCassaJar {


	public static void updateConfigValue(String suffixValue){
		Process cmd;
		Runtime re = Runtime.getRuntime();
		BufferedReader output = null;
		String resultOutput="";
		try
		{ 
		  cmd = re.exec("java -jar cassa.jar write "+suffixValue); 
		  output =  new BufferedReader(new InputStreamReader(cmd.getInputStream()));
		  resultOutput = output.readLine();
		  System.out.println("resultOutput -->"+resultOutput);
		} 
		catch (IOException e){
		  e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		//return resultOutput;
	}

	public static String readConfigValue(String suffixkey){
		String suffixValue="";
		Process cmd;
		Runtime re = Runtime.getRuntime();
		BufferedReader output = null;
		String resultOutput="";
		try
		{ 
		  cmd = re.exec("java -jar cassa.jar read "+suffixkey);
		  output =  new BufferedReader(new InputStreamReader(cmd.getInputStream()));
		  resultOutput = output.readLine();
		  //below hack is to skip all output lines (e.g logger statements) until it find the text '_1 or _2'
          while ((resultOutput = output.readLine()) != null) {
              //System.out.println(resultOutput);
              suffixValue = resultOutput;
         }
		} 
		catch (IOException e){
		  e.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		return suffixValue;
	}
	
	
	public static void main(String[] args) {
		
		System.out.println("Current suffixValue: "+ readConfigValue("suffix-key"));
		//System.out.println("Updating config Value ..");
		//updateConfigValue("_1");
		
	}
}
