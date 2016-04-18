/**
 * Indexing records to Solr.
 * This is where the actual importing process is handled.
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
package betullam.akimporter.solrmab;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import betullam.akimporter.main.Main;
import betullam.akimporter.solrmab.indexing.Mabfield;
import betullam.akimporter.solrmab.indexing.MarcContentHandler;
import betullam.akimporter.solrmab.indexing.MatchingObject;


public class Index {

	private HttpSolrServer solrServer;
	private String mabXMLfile;
	private String mabPropertiesFile;
	private List<MatchingObject> listOfMatchingObjs; // Contents from mab.properties file
	private boolean useDefaultMabProperties = true;
	public String pathToTranslationFiles;
	private boolean print = true;
	private long startTime;
	private long endTime;
	boolean optimizeSolr = true;
	private String timeStamp = null;
	private SolrMabHelper smHelper;
	private boolean isIndexingSuccessful = false;

	public static List<String> multiValuedFields = new ArrayList<String>();
	public static List<Mabfield> customTextFields = new ArrayList<Mabfield>();
	public static HashMap<String, List<String>> translateFields = new HashMap<String, List<String>>();



	public Index(String mabXmlFile, HttpSolrServer solrServer, boolean useDefaultMabProperties, String mabPropertiesFile, String pathToTranslationFiles, String timeStamp, boolean optimizeSolr, boolean print) {
		this.mabXMLfile = mabXmlFile;
		this.solrServer = solrServer;
		this.useDefaultMabProperties = useDefaultMabProperties;
		this.mabPropertiesFile = mabPropertiesFile;
		this.pathToTranslationFiles = pathToTranslationFiles;
		this.timeStamp = timeStamp;
		this.optimizeSolr = optimizeSolr;
		this.print = print;
		this.smHelper = new SolrMabHelper(solrServer);

		this.startIndexing();
	};

	/**
	 * Starting the index process.
	 */
	private void startIndexing() {

		try {
			BufferedInputStream mabPropertiesInputStream = null;

			// Load .properties file:
			if (useDefaultMabProperties) {
				mabPropertiesInputStream = new BufferedInputStream(Main.class.getResourceAsStream(this.mabPropertiesFile));
			} else {
				mabPropertiesInputStream = new BufferedInputStream(new FileInputStream(this.mabPropertiesFile));
			}


			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			//++++++++++++++++++++++++++++++++++ PARSING & INDEXING +++++++++++++++++++++++++++++++++//
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

			startTime = System.currentTimeMillis();

			// Get contents of mab.properties files and put them to MatchingObjects
			listOfMatchingObjs = getMatchingObjects(mabPropertiesInputStream, pathToTranslationFiles);

			// Create SAX parser:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();

			// Specify XML-file to parse. These are our bibliographic data from Aleph Publisher:
			FileReader reader = new FileReader(mabXMLfile);
			InputSource inputSource = new InputSource(reader);

			// Set ContentHandler:
			MarcContentHandler marcContentHandler = new MarcContentHandler(listOfMatchingObjs, this.solrServer, this.timeStamp, this.print);
			xmlReader.setContentHandler(marcContentHandler);

			// Start parsing & indexing:
			xmlReader.parse(inputSource);
			this.smHelper.print(print, "\n");

			// Commit records:
			this.solrServer.commit();

			isIndexingSuccessful = true;

			if (optimizeSolr) {
				this.smHelper.print(print, "Start optimizing Solr index. This could take a while. Please wait ...\n");
				this.smHelper.solrOptimize();
				this.smHelper.print(print, "Done optimizing Solr index.\n\n");
			}
			endTime = System.currentTimeMillis();
			smHelper.print(print, "Done indexing to solr. Execution time: " + smHelper.getExecutionTime(startTime, endTime) + "\n");

			isIndexingSuccessful = true;

		} catch (RemoteSolrException e) {
			isIndexingSuccessful = false;
			System.out.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.out.println("Solr error! Please check Solr-Setting in general.properties file. Maybe it's not set correctly! E. g. it could be a typo. Check also if the Solr-Server is up and running!");
			System.out.println("\n\nSee also StackTrace:\n");
			e.printStackTrace();
			System.out.println("\n-----------------------------------------------------------------------\n");
		} catch (FileNotFoundException e) {
			isIndexingSuccessful = false;
			System.out.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.out.println("File error! Most possible reasons for this error:\n");
			System.out.println("\n1. The \"general.properties\"-file is not in directory you are right now (at the moment you are in " + System.getProperty("user.dir") + "). Please change the command promt to the directory where the \"general.properties\"-file is. Also check if it's named correctly.");
			System.out.println("\n2. The XML-file with the data from Aleph is not at the location that is specified in the general.properties-file (check \"mabXMLfile\" there).");
			System.out.println("\n\nSee also StackTrace:\n");
			e.printStackTrace();
			System.out.println("\n-----------------------------------------------------------------------\n");
		} catch (IOException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (Exception e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		}
	}

	/**
	 * Checks if the index process was successful.
	 * @return	true if the index process was successful.
	 */
	public boolean isIndexingSuccessful() {
		return isIndexingSuccessful;
	}


	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++ MAB PROPERTIES ++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

	/**
	 * Getting the rules defined in mab.properties file.
	 * 
	 * @param propertiesStream			Input stream of the mab.properties file.
	 * @param pathToTranslationFiles	Path to the directory where the translation files are stored.
	 * @return							The rules of defined in mab.properties file represented as a list of MatchingObjects
	 */
	private List<MatchingObject> getMatchingObjects(BufferedInputStream propertiesStream, String pathToTranslationFiles) {

		/*
		System.out.println("\n\n-------------------------------------------");
		String test = "123$**$a, contains[test], regEx[[\\d],te\\[st], test, regEx[[\\]\\[est]]";
		test = test.replace("\\[", "").replace("\\]", "");
		String newTest = "";
		int openBracketsCounter = 0;
		int closeBracketsCounter = 0;
		int bracketCounter = 0;
		// Iterate over each character of a line in the properties file:
		for (int i = 0; i < test.length(); i++){
		    char c = test.charAt(i);
		    String s = Character.toString(c);
		    // Check if the current character is an opening bracket
		    if (s.equals("[")) {
		    	openBracketsCounter = openBracketsCounter + 1;
		    	bracketCounter = bracketCounter + 1;
		    }
		    // Add characters to the new string only if not within an opening bracket
		    if (bracketCounter <= 0) {
		    	newTest += s;
		    }
		    // Check if the current character is a closing bracket
		    if (s.equals("]")) {
		    	closeBracketsCounter = closeBracketsCounter + 1;
		    	bracketCounter = bracketCounter - 1;
		    }
		}
		
	    // If there are not as many opening brackets as closing brackets, there is an error in the syntax
		if (openBracketsCounter != closeBracketsCounter) {
			System.err.println("Please check in our mab properties file if you forgot an opening [ or closing ] square bracket. "
					+ "If a square bracket is part of your desired matching result in a regEx rule, be sure to escape it with a double backslash, e. g.: \\\\[");
			System.exit(0);
		}
		System.out.println(newTest);
		System.out.println("\n-------------------------------------------\n");
		*/
		
		
		List<MatchingObject> matchingObjects = new ArrayList<MatchingObject>();

		try {
			Properties mabProperties = new Properties();

			// Load contents of properties-file:
			mabProperties.load(propertiesStream);
			propertiesStream.close();

			// Loop through properties, put them into MatcingObjects and add them to a List<MatchingObject>:
			for(String key : mabProperties.stringPropertyNames()) {
				boolean multiValued = false;
				boolean customText = false;
				boolean translateValue = false;
				boolean translateValueContains = false;
				boolean hasDefaultValue = false;
				boolean hasRegex = false;
				String defaultValue = null;
				String regexValue = null;
				String strValues = mabProperties.getProperty(key);
				
				// Removing everything between square brackets to get a clean string with mab property rules for proper working further down.
				// INFO:
				// We can't use something like replaceAll or replaceFirst becaus in regEx rules, we could have nested brackets, e. g.
				// regEX[test[\\d]test\\[test]. This would cause problems with replaceAll or replaceFirst. That's why we will iterate over
				// each character of the string and check for opening and closing brackets while counting them. We will then create a new
				// String by adding one character after another of the original string, except the characters between outer brackets with
				// count value 0.
				String strValuesNoRegexBrackets = strValues.replace("\\[", "").replace("\\]", "");
				String strValuesClean = "";
				int openBracketsCounter = 0;
				int closeBracketsCounter = 0;
				int bracketCounter = 0;
				// Iterate over each character of a line in the properties file:
				for (int i = 0; i < strValuesNoRegexBrackets.length(); i++){
				    char c = strValuesNoRegexBrackets.charAt(i);
				    String s = Character.toString(c);
				    // Check if the current character is an opening bracket
				    if (s.equals("[")) {
				    	openBracketsCounter = openBracketsCounter + 1;
				    	bracketCounter = bracketCounter + 1;
				    }
				    // Add characters to the new string only if not within an outer bracket (count value 0)
				    if (bracketCounter <= 0) {
				    	strValuesClean += s;
				    }
				    // Check if the current character is a closing bracket
				    if (s.equals("]")) {
				    	closeBracketsCounter = closeBracketsCounter + 1;
				    	bracketCounter = bracketCounter - 1;
				    }
				}
				
			    // If there are not as many opening brackets as closing brackets, there is an error in the syntax
				if (openBracketsCounter != closeBracketsCounter) {
					System.err.println("Please check in our mab properties file if you forgot an opening [ or closing ] square bracket. "
							+ "If a square bracket is part of your desired matching result in a regEx rule, be sure to escape it with a double backslash, e. g.: \\\\[");
					System.exit(0);
				}
				
				
				HashMap<String, String> translateProperties = new HashMap<String, String>();
				String filename = null;
				HashMap<String, List<String>> mabFieldnames = new HashMap<String, List<String>>();
				List<String> fieldsToRemove = new ArrayList<String>();

				// Create CHANGABLE list:
				List<String> lstValues = new ArrayList<String>();
				lstValues.addAll(Arrays.asList(strValues.split("\\s*,\\s*")));

				// Create a clean list (without square brackets) for option check below. This is in case a translateValue
				// uses the default text option, e. g. translateValueContains[MyDefaultText]. The function
				// "lstValues.contains("translateValueContains") would not match with translateValueContains[MyDefaultText].
				// Also if regex[REGEX] is used. It would not match with the square brackets!
				List<String> lstValuesClean = new ArrayList<String>();
				lstValuesClean.addAll(Arrays.asList(strValuesClean.split("\\s*,\\s*")));

				// Check for options and prepare values
				if (lstValuesClean.contains("multiValued")) {
					multiValued = true;
					lstValues.remove(lstValuesClean.indexOf("multiValued")); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(lstValuesClean.indexOf("multiValued")); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations. 
				}
				if (lstValuesClean.contains("customText")) {
					customText = true;
					lstValues.remove(lstValuesClean.indexOf("customText")); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(lstValuesClean.indexOf("customText")); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
				}


				if (lstValuesClean.contains("translateValue") || lstValuesClean.contains("translateValueContains")) {
					int index = 0;

					
					// Is translateValue
					if (lstValuesClean.contains("translateValue")) {
						translateValue = true;
						translateValueContains = false;
						index = lstValuesClean.indexOf("translateValue");
						lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
						lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					}

					// Is translateValueContains
					if (lstValuesClean.contains("translateValueContains")) {
						translateValueContains = true;
						translateValue = false;
						index = lstValuesClean.indexOf("translateValueContains");
						lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
						lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					}

					// Get the filename with the help of RegEx:
					Pattern patternPropFile = java.util.regex.Pattern.compile("[^\\s,;]*\\.properties"); // No (^) whitespaces (\\s), commas or semicolons (,;) before ".properties"-string.
					Matcher matcherPropFile = patternPropFile.matcher("");
					for(String lstValue : lstValues) {
						matcherPropFile.reset(lstValue);
						if(matcherPropFile.find()) {
							filename = matcherPropFile.group();
						}
					}
					lstValues.remove(filename);
					lstValuesClean.remove(filename);
				}

				if (lstValuesClean.contains("defaultValue")) {
					String defaultValueString = null;
					hasDefaultValue = true;
					int index = lstValuesClean.indexOf("defaultValue");
					defaultValueString =  lstValues.get(index); // Get whole string of defaultValue incl. square brackets, e. g. defaultValue[DefaultValue]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.

					if (defaultValueString != null) {
						// Extract the default value in the square brackets:
						Pattern patternDefaultValue = java.util.regex.Pattern.compile("\\[.*?\\]"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcherDefaultValue = patternDefaultValue.matcher(defaultValueString);
						defaultValue = (matcherDefaultValue.find()) ? matcherDefaultValue.group().replace("[", "").replace("]", "").trim() : null;
					}
				}


				if (lstValuesClean.contains("regEx")) {
					String regexValueString = null;
					hasRegex = true;
					int index = lstValuesClean.indexOf("regEx");
					regexValueString =  lstValues.get(index); // Get whole string of regex value incl. square brackets, e. g. regEx[REGEX]
					lstValues.remove(index); // Use index of clean list (without square brackets).
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					if (regexValueString != null) {
						// Extract the regex value in the square brackets:
						Pattern patternRegexValue = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcherRegexValue = patternRegexValue.matcher(regexValueString);
						regexValue = (matcherRegexValue.find()) ? matcherRegexValue.group().replaceFirst("\\[", "").replaceFirst("\\]$", "").trim() : null;
					}
				}


				// Get all multiValued fields and remove them after we finished:
				if (multiValued) {
					for(String lstValue : lstValues) {

						// Remove  square brackets from the mabfield name (in case it is also a translateValue field) so that
						// we have a clear mabfield-name to process. If not, it wouldn't work in the matching operations!
						String cleanLstValue = lstValue.replaceAll("\\[.*?\\]", "");
						mabFieldnames.put(cleanLstValue, null);

						// Do not remove field if it's also a translateValue or translateValueContains (then just add "null"),
						// because in this case we will still need the fields further down for the translate operations.
						fieldsToRemove.add((translateValue || translateValueContains) ? null : lstValue);
					}
					lstValues.removeAll(fieldsToRemove);
					fieldsToRemove.clear();
				}


				// Get all customText fields and remove them after we finished:
				if (customText) {
					for(String lstValue : lstValues) {
						mabFieldnames.put(lstValue, null);
						fieldsToRemove.add(lstValue);
					}
					lstValues.removeAll(fieldsToRemove);
					fieldsToRemove.clear();
				}

				// Get all translateValue and translateValueContains fields and remove them after we finished:
				if (translateValue || translateValueContains) {

					if (filename != null) {

						// Get the mapping values from .properties file:
						translateProperties = getTranslateProperties(filename, pathToTranslationFiles);

						// Get the count of characters that should be matched (e. g. 051[1-3]: get 1 and 3) and add it to a List<String>.
						// Then add everything to a HashMap<String, List<String>>.
						String from = "";
						String to = "";
						String all = "";

						Pattern patternFrom = Pattern.compile("(?<=\\[)\\d*");
						Pattern patternTo = Pattern.compile("\\d*(?=\\])");
						Pattern patternAll = Pattern.compile("(all)");
						Matcher matcherFrom = patternFrom.matcher("");
						Matcher matcherTo = patternTo.matcher("");
						Matcher matcherAll = patternAll.matcher("");


						for(String lstValue : lstValues) {

							// Get the numbers of the characters in square brackets (e. g. 051[1-3]: get 1 and 3) or "all" (e. g. 051[all]):
							List<String> fromTo = new ArrayList<String>();
							matcherAll.reset(lstValue);
							all = (matcherAll.find()) ? matcherAll.group() : "";

							if (!all.isEmpty() && all != "") {
								fromTo.add("all");
								fromTo.add("all");
							} else {
								matcherFrom.reset(lstValue);
								from = (matcherFrom.find()) ? matcherFrom.group() : "";
								matcherTo.reset(lstValue);
								to = (matcherTo.find()) ? matcherTo.group() : "";
								fromTo.add(from);
								fromTo.add(to);
							}

							// Remove the square brackets from the mabfield name so that we have a clear mabfield-name:
							String cleanLstValue = lstValue.replaceAll("\\[.*?\\]", "");

							// Add the values to the Mabfield HashMap:
							mabFieldnames.put(cleanLstValue, fromTo);
							fieldsToRemove.add(lstValue);
						}

						lstValues.removeAll(fieldsToRemove);
						fieldsToRemove.clear();

					} else {
						System.out.println("Error: You need to specify a translation-properties file with the file-ending \".properties\"!");
					}
				}				

				// Get all default fields (the other fields were removed):
				for(String lstValue : lstValues) {
					mabFieldnames.put(lstValue, null);
					fieldsToRemove.add(lstValue);
				}
				lstValues.removeAll(fieldsToRemove);
				fieldsToRemove.clear();

				MatchingObject mo = new MatchingObject(key, mabFieldnames, multiValued, customText, translateValue, translateValueContains, translateProperties, hasDefaultValue, defaultValue, hasRegex, regexValue);				
				matchingObjects.add(mo);
			}

		} catch (IOException e) {
			matchingObjects = null;			
			System.out.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.out.println("File error! The mab.properties-file was not found. Please check \"mabPropertiesFile\"-setting in general.properties file. Maybe it's not set correctly! E. g. it could be a typo.");
			System.out.println("\nSee also StackTrace:\n");
			e.printStackTrace();
			System.out.println("\n-----------------------------------------------------------------------\n");
		}

		for (MatchingObject matchingObject : matchingObjects) {
			if (matchingObject.isMultiValued()) {
				multiValuedFields.add(matchingObject.getSolrFieldname());
			}

			if (matchingObject.isCustomText()) {
				String solrFieldName = matchingObject.getSolrFieldname();
				HashMap<String, List<String>> customTexts = matchingObject.getMabFieldnames();

				// Make new Mabfield for each custom text and add it to a List of Mabfields, so we can process it later on:
				for (Entry<String, List<String>> customText : customTexts.entrySet()) {
					customTextFields.add(new Mabfield(solrFieldName, customText.getKey()));
				}
			}

			if (matchingObject.isTranslateValue() || matchingObject.isTranslateValueContains()) {
				translateFields = matchingObject.getMabFieldnames();
			}

		}

		return matchingObjects;
	}


	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++ TRANSLATE PROPERTIES ++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

	/**
	 * Getting the rules defined in a translation file.
	 * 
	 * @param filename					File name of the translation file.
	 * @param pathToTranslationFiles	Path to the directory where the translation files are stored.
	 * @return							A HashMap<String, String> representing the rules defined in a translation file.
	 */
	private HashMap<String, String> getTranslateProperties(String filename, String pathToTranslationFiles) {

		HashMap<String, String> translateProperties = new HashMap<String, String>();

		Properties properties = new Properties();
		String translationFile = pathToTranslationFiles + File.separator + filename;
		BufferedInputStream translationStream = null;

		try {
			// Get .properties file and load contents:
			if (useDefaultMabProperties) {
				translationStream = new BufferedInputStream(Main.class.getResourceAsStream("/betullam/akimporter/resources/" + filename));
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
}
