package com.wmg.utilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Reader {

	public static void main(String args[]){
	
		
		String s = null;

        try {
            
            Process p = Runtime.getRuntime().exec("sudo -u wmgload hadoop jar /usr/local/code/musicmetric/BulkLoader.jar /data/raw/musicmetrics/airplay/* 10.240.171.68 MusicMetricData airplay_2");
            
            BufferedReader stdInput = new BufferedReader(new 
                 InputStreamReader(p.getInputStream()));

            BufferedReader stdError = new BufferedReader(new 
                 InputStreamReader(p.getErrorStream()));

            // read the output from the command
            System.out.println("Here is the standard output of the command:\n");
            while ((s = stdInput.readLine()) != null) {
                System.out.println("stdInput readLine");
            	System.out.println(s);
            }
            
            // read any errors from the attempted command
            System.out.println("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
            	System.out.println("stdError readLine");
                System.out.println(s);
            }
            
            
            stdError.close();
            stdInput.close();
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("exception happened - here's what I know: ");
            e.printStackTrace();
            System.exit(-1);
        }
        
        
/*		//String suffixValue="";
		Process cmd;
		Runtime re = Runtime.getRuntime();
		BufferedReader output = null;
		String resultOutput="";
		try
		{ 
			// sudo -u hdfs hadoop fs -ls /data/raw/musicmetrics/

		  //cmd = re.exec("sudo -u hdfs hadoop fs -ls /data/raw/musicmetrics/");
		  
		  cmd = re.exec("sudo -u wmgload hadoop jar /usr/local/code/musicmetric/BulkLoader.jar /data/raw/musicmetrics/amazon_sales/* 10.240.171.68 MusicMetricData Amazon_2");
				  
		  output =  new BufferedReader(new InputStreamReader(cmd.getInputStream()));
		  //resultOutput = output.readLine();
          while ((resultOutput = output.readLine()) != null) {
              System.out.println("resultOutput -->"+resultOutput);
         }
          
          System.out.println("completed hadoop jar command");
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
		*/
		
	}
	
}
