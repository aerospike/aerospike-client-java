/* 
 * Copyright 2012-2014 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.benchmarks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {	
	/**
	 * Read all the contents from the file and put it in a List.
	 * @throws IOException 
	 */
	protected static List<String> readKeyFromFile(String filepath) throws IOException {
		List<String> contentsFromFile = readAllLines(filepath);
		return contentsFromFile;
	}

    private static List<String> readAllLines(String filepath) throws IOException {
    	List<String> fileContent = new ArrayList<String>();
        File file = new File(filepath);
        
        BufferedReader input =  new BufferedReader(new FileReader(file));
        try {
        	String line = null; 
        	while (( line = input.readLine()) != null) {
            	  fileContent.add(line);
            }
        }
        finally {
        	if (input != null)
        		input.close();
        }
        
        return fileContent;
    }
    
    public static boolean isNumeric(String str) {
	  return str.matches("-?\\d+(\\.\\d+)?");  //match a number with optional '-' and decimal.
	}

}
