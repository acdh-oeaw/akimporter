/**
 * Helper class for indexing data to Solr.
 *  
 * Copyright (C) AK Bibliothek Wien 2016, Michael Birkner
 * 
 * This file is part of AkImporter.
 * 
 * AkImporter is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AkImporter is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AkImporter.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Michael Birkner <michael.birkner@akwien.at>
 * @license  http://www.gnu.org/licenses/gpl-3.0.html
 * @link     http://wien.arbeiterkammer.at/service/bibliothek/
 */
package main.java.betullam.akimporter.main;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class AkImporterHelper {

	
	/**
	 * Getting values between square brackets as Map.
	 * 
	 * @param rawValue			String: The raw value with square brackets, e. g. connectedSubfields[b:4:NoRole][9:NoGndId]
	 * @return					Map<Integer, String>: Integer indicates the position of the square bracket, String is the value within the bracket
	 */
	public static LinkedHashMap<Integer, String> getBracketValues(String rawValue) {		
		LinkedHashMap<Integer, String> bracketValues = new LinkedHashMap<Integer, String>();

		String valueClean = "";
		int outerBracketsCounter = 0;
		int openBracketsCounter = 0; // Reuse variable from above
		int closeBracketsCounter = 0; // Reuse variable from above
		int bracketCounter = 0; // Reuse variable from above

		// Iterate over each character of rawValue:
		for (int i = 0; i < rawValue.length(); i++){
			char c = rawValue.charAt(i);
			String s = Character.toString(c);
			// Check if the current character is an opening bracket
			if (s.equals("[")) {
				openBracketsCounter = openBracketsCounter + 1;
				bracketCounter = bracketCounter + 1;
				// Check if we have an outer bracket (count value equals 1) {
				if (bracketCounter == 1) {
					outerBracketsCounter = outerBracketsCounter + 1;
				}
			}
			// Add characters to the new string only if within an outer bracket (count value equals or higher 1)
			if (bracketCounter >= 1) {
				valueClean += s;
			}
			// Check if the current character is a closing bracket
			if (s.equals("]")) {
				if (bracketCounter == 1) {								
					bracketValues.put(outerBracketsCounter, valueClean.replaceFirst("\\[", "").replaceFirst("\\]$", ""));
					valueClean = "";
				}
				closeBracketsCounter = closeBracketsCounter + 1;
				bracketCounter = bracketCounter - 1;		
			}
		}

		return bracketValues;
	}
	
	
	/**
	 * Getting the rules defined in a translation file.
	 * 
	 * @param filename					String: File name of the translation file.
	 * @param pathToTranslationFiles	String: Path to the directory where the translation files are stored.
	 * @return							A HashMap<String, String> representing the rules defined in a translation file.
	 */
	public static HashMap<String, String> getTranslateProperties(String filename, String pathToTranslationFiles, boolean useDefaultProperties) {

		HashMap<String, String> translateProperties = new HashMap<String, String>();

		Properties properties = new Properties();
		String translationFile = pathToTranslationFiles + File.separator + filename;
		BufferedInputStream translationStream = null;

		try {
			// Get .properties file and load contents:
			if (useDefaultProperties) {
				translationStream = new BufferedInputStream(Main.class.getResourceAsStream("/main/resources/" + filename));
			} else {
				translationStream = new BufferedInputStream(new FileInputStream(translationFile));
			}
			properties.load(translationStream);
			translationStream.close();
		} catch (FileNotFoundException e) {
			System.err.println("Error: File not found! Please check if the file \"" + translationFile + "\" is in the same directory as mab.properties.\n");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (Map.Entry<?, ?> property : properties.entrySet()) {
			String key = (String)property.getKey();
			String value = (String)property.getValue();
			translateProperties.put(key, value);
		}

		return translateProperties;
	}
	

	/**
	 * Get human readable execution time between two moments in time expressed in milliseconds.
	 * @param startTime		long: Moment of start in milliseconds
	 * @param endTime		long: Moment of end in milliseconds
	 * @return				String of human readable execution time.
	 */
	public static String getExecutionTime(long startTime, long endTime) {
		String executionTime = null;

		long timeElapsedMilli =  endTime - startTime;
		int seconds = (int) (timeElapsedMilli / 1000) % 60 ;
		int minutes = (int) ((timeElapsedMilli / (1000*60)) % 60);
		int hours   = (int) ((timeElapsedMilli / (1000*60*60)) % 24);

		executionTime = hours + ":" + minutes + ":" + seconds;
		return executionTime;
	}

	
	/**
	 * Starts an optimize action for a Solr core.
	 * @param solrServer	HttpSolrServer: the Solr server that needs to be optimized.
	 */
	public static void solrOptimize(HttpSolrServer solrServer) {
		try {
			solrServer.optimize();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Prints a text message to the console if "print" is true.
	 * @param print		boolean: True if the message should be printed.
	 * @param text		String: The text to print to the console.
	 */
	public static void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}
}