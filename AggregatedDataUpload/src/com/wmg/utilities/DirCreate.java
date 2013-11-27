package com.wmg.utilities;

import java.io.File;

public class DirCreate {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		//ParentDir/ChildDir
		
		String parentDir="ParentDir";
		String childDir="childDir";
		
        File parentDirectory = new File(parentDir);
        File childDirectory = new File(parentDir+"\\"+childDir);
        
        if (!parentDirectory.exists())
        {
        	parentDirectory.mkdir();
        	System.out.println("created parnt dir");
        }
        if (!childDirectory.exists())
        {
        	childDirectory.mkdir();
        	System.out.println("created child dir");
        }
        

	}

}
