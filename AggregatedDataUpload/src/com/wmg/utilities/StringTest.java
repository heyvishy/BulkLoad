package com.wmg.utilities;

import java.util.Arrays;

public class StringTest {

	public static void main(String[] args) {
		
		
		System.out.println("Starting String test...");

		
        String[] params =  new String[3];
        params[0] = "sudo";
        params[1] = "-u";
        params[2] ="hdfs";

		System.out.println("params -->"+Arrays.toString(params));
		
		StringBuilder builder = new StringBuilder();
		for(String s : params) {
			builder.append(" ");
		    builder.append(s);
		}
		
		System.out.println("string entie "+builder.toString());
		
	}

}
