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
package main.java.betullam.akimporter.solrmab;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.main.Main;
import main.java.betullam.akimporter.solrmab.indexing.Controlfield;
import main.java.betullam.akimporter.solrmab.indexing.Datafield;
import main.java.betullam.akimporter.solrmab.indexing.Leader;
import main.java.betullam.akimporter.solrmab.indexing.MarcContentHandler;
import main.java.betullam.akimporter.solrmab.indexing.PropertiesObject;
import main.java.betullam.akimporter.solrmab.indexing.SolrField;
import main.java.betullam.akimporter.solrmab.indexing.Subfield;

// TODO: This file is a mess. Clean it up!

public class Index {

	private HttpSolrServer solrServer = null;
	private String mabXMLfile = null;
	private String mabPropertiesFile = null;
	private List<PropertiesObject> listOfMatchingObjs = null; // Contents from mab.properties file
	private boolean useDefaultMabProperties = true;
	public String pathToTranslationFiles = null;
	private boolean print = true;
	private long startTime;
	private long endTime;
	boolean optimizeSolr = true;
	private String timeStamp = null;
	private boolean isIndexingSuccessful = false;
	public static List<SolrField> customTextFields = new ArrayList<SolrField>();
	private boolean indexSampleData = false;

	// Default Constructor
	public Index(String mabXmlFile, HttpSolrServer solrServer, boolean useDefaultMabProperties, String mabPropertiesFile, String pathToTranslationFiles, String timeStamp, boolean optimizeSolr, boolean print) {
		this.mabXMLfile = mabXmlFile;
		this.solrServer = solrServer;
		this.useDefaultMabProperties = useDefaultMabProperties;
		this.mabPropertiesFile = mabPropertiesFile;
		this.pathToTranslationFiles = pathToTranslationFiles;
		this.timeStamp = timeStamp;
		this.optimizeSolr = optimizeSolr;
		this.print = print;

		this.startIndexing();
	};
	
	// Constructor for indexing sample data:
	public Index(boolean indexSampleData, HttpSolrServer solrServer, boolean useDefaultMabProperties, String mabPropertiesFile, String pathToTranslationFiles, String timeStamp, boolean optimizeSolr, boolean print) {
		this.indexSampleData = indexSampleData;
		this.solrServer = solrServer;
		this.useDefaultMabProperties = useDefaultMabProperties;
		this.mabPropertiesFile = mabPropertiesFile;
		this.pathToTranslationFiles = pathToTranslationFiles;
		this.timeStamp = timeStamp;
		this.optimizeSolr = optimizeSolr;
		this.print = print;

		this.startIndexing();
	};

	/**
	 * Starting the index process.
	 */
	private void startIndexing() {

		BufferedInputStream mabPropertiesInputStream = null;
		BufferedInputStream xmlSampleDataStream = null;
		FileReader reader = null;
		try {

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
			InputSource inputSource = null;
			if (indexSampleData) {
				xmlSampleDataStream = new BufferedInputStream(Main.class.getResourceAsStream("/main/resources/sampledata_aksearch.xml"));	
				inputSource = new InputSource(xmlSampleDataStream);
			} else {
				reader = new FileReader(mabXMLfile);
				inputSource = new InputSource(reader);
			}
			
			// Set ContentHandler:
			MarcContentHandler marcContentHandler = new MarcContentHandler(listOfMatchingObjs, this.solrServer, this.timeStamp, this.print);
			xmlReader.setContentHandler(marcContentHandler);

			// Start parsing & indexing:
			xmlReader.parse(inputSource);
			AkImporterHelper.print(print, "\n");

			// Commit records:
			this.solrServer.commit();

			isIndexingSuccessful = true;

			if (optimizeSolr) {
				AkImporterHelper.print(print, "Start optimizing Solr index. This could take a while. Please wait ...\n");
				AkImporterHelper.solrOptimize(this.solrServer);
				AkImporterHelper.print(print, "Done optimizing Solr index.\n");
			}
			endTime = System.currentTimeMillis();
			AkImporterHelper.print(print, "Done indexing to Solr. Execution time: " + AkImporterHelper.getExecutionTime(startTime, endTime) + "\n\n");

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
		} finally {
			// Close all streams and readers and set variables to null to free memory
			try {
				if (mabPropertiesInputStream != null) { mabPropertiesInputStream.close(); }
				if (xmlSampleDataStream != null) { xmlSampleDataStream.close(); }
				if (reader != null) { reader.close(); }
			} catch (IOException e) {
				e.printStackTrace();
			}
			listOfMatchingObjs = null;
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
	private List<PropertiesObject> getMatchingObjects(BufferedInputStream propertiesStream, String pathToTranslationFiles) {

		List<PropertiesObject> propertiesObjects = new ArrayList<PropertiesObject>();

		try {

			Properties mabProperties = new Properties();

			// Load contents of properties-file:
			mabProperties.load(propertiesStream);
			propertiesStream.close();

			// Loop through properties, put them into MatcingObjects and add them to a List<MatchingObject>:
			for(String key : mabProperties.stringPropertyNames()) {

				Leader leader = null;
				List<Datafield> datafields = new ArrayList<Datafield>();
				List<Controlfield> controlfields = new ArrayList<Controlfield>();
				boolean multiValued = false;
				boolean customText = false;
				boolean getAllFields = false;
				List<String> allFieldsExceptions = new ArrayList<String>();
				boolean getFullRecordAsXML = false;
				boolean translateValue = false;
				boolean translateValueContains = false;
				boolean translateValueRegex = false;
				boolean hasDefaultValue = false;
				boolean hasRegex = false;
				boolean hasRegexStrict = false;
				boolean hasRegExReplace = false;
				String defaultValue = null;
				LinkedHashMap<Integer, String> connectedSubfields = new LinkedHashMap<Integer, String>();
				boolean hasConnectedSubfields = false;
				boolean translateConnectedSubfields = false;
				LinkedHashMap<Integer, String> concatenatedSubfields = new LinkedHashMap<Integer, String>();
				String concatenatedSubfieldsSeparator = null;
				boolean hasConcatenatedSubfields = false;
				boolean translateConcatenatedSubfields = false;
				String regexValue = null;
				String regexStrictValue = null;
				String regexReplaceValue = null;
				Map<Integer, String> regexReplaceValues = new HashMap<Integer, String>();
				String strValues = mabProperties.getProperty(key);
				boolean allowDuplicates = false;
				boolean hasSubfieldExists = false;
				LinkedHashMap<Integer, String> subfieldExists = new LinkedHashMap<Integer, String>();
				boolean hasSubfieldNotExists = false;
				LinkedHashMap<Integer, String> subfieldNotExists = new LinkedHashMap<Integer, String>();
				LinkedHashMap<String, List<String>> applyToFields = new LinkedHashMap<String, List<String>>();


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
				HashMap<String, String> translateConnectedSubfieldsProperties = new HashMap<String, String>();
				HashMap<String, String> translateConcatenatedSubfieldsProperties = new HashMap<String, String>();
				String filename = null;
				HashMap<String, List<String>> mabFieldnames = new HashMap<String, List<String>>();
				List<String> fieldsToRemove = new ArrayList<String>();

				// Create CHANGABLE list:
				List<String> lstValues = new ArrayList<String>();
				//lstValues.addAll(Arrays.asList(strValues.split("\\s*,\\s*")));
				lstValues.addAll(Arrays.asList(strValues.split("\\s*(?<!\\\\),\\s*")));


				// Create a clean list (without square brackets) for option check below. This is in case a translateValue
				// uses the default text option, e. g. translateValueContains[MyDefaultText]. The function
				// "lstValues.contains("translateValueContains") would not match with translateValueContains[MyDefaultText].
				// Also if regex[REGEX] is used. It would not match with the square brackets!
				List<String> lstValuesClean = new ArrayList<String>();
				lstValuesClean.addAll(Arrays.asList(strValuesClean.split("\\s*(?<!\\\\),\\s*")));

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

				if (lstValuesClean.contains("getAllFields")) {
					getAllFields = true;
					String getAllFieldsString = null;
					int index = lstValuesClean.indexOf("getAllFields");
					getAllFieldsString = lstValues.get(index); // Get whole string incl. square brackets, e. g. getAllFields[url:id:...]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					if (getAllFieldsString != null) {
						// Extract the text in the square brackets:
						Pattern patternAllFieldsExceptions = java.util.regex.Pattern.compile("\\[.*?\\]"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcherAllFieldsExceptions = patternAllFieldsExceptions.matcher(getAllFieldsString);
						String strAllFieldsExceptions = (matcherAllFieldsExceptions.find()) ? matcherAllFieldsExceptions.group().replace("[", "").replace("]", "").trim() : null;
						allFieldsExceptions = Arrays.asList(strAllFieldsExceptions.split("\\s*:\\s*"));
					}
				}

				if (lstValuesClean.contains("getFullRecordAsXML")) {
					getFullRecordAsXML = true;
					lstValues.remove(lstValuesClean.indexOf("getFullRecordAsXML")); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(lstValuesClean.indexOf("getFullRecordAsXML")); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations. 
				}

				if (lstValuesClean.contains("translateValue") || lstValuesClean.contains("translateValueContains") || lstValuesClean.contains("translateValueRegex")) {
					int index = 0;

					// Is translateValue
					if (lstValuesClean.contains("translateValue")) {
						translateValue = true;
						translateValueContains = false;
						translateValueRegex = false;
						index = lstValuesClean.indexOf("translateValue");
						lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
						lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					}

					// Is translateValueContains
					if (lstValuesClean.contains("translateValueContains")) {
						translateValueContains = true;
						translateValue = false;
						translateValueRegex = false;
						index = lstValuesClean.indexOf("translateValueContains");
						lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
						lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					}

					// Is translateValueRegex
					if (lstValuesClean.contains("translateValueRegex")) {
						translateValueRegex = true;
						translateValue = false;
						translateValueContains = false;
						index = lstValuesClean.indexOf("translateValueRegex");
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

				if (lstValuesClean.contains("connectedSubfields")) {
					String connectedSubfieldsString = null;
					int index = lstValuesClean.indexOf("connectedSubfields");
					connectedSubfieldsString = lstValues.get(index).trim(); // Get whole string incl. square brackets, e. g. connectedSubfields[a:b:c]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					hasConnectedSubfields = true;
					if (connectedSubfieldsString != null) {
						// Extract the text in the square brackets:
						Pattern patternConnectedSubfields = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between the first and last squary brackets
						Matcher matcherConnectedSubfields = patternConnectedSubfields.matcher(connectedSubfieldsString);
						String connectedSubfieldsAllBrackets = (matcherConnectedSubfields.find()) ? matcherConnectedSubfields.group().trim() : null;
						connectedSubfieldsAllBrackets = connectedSubfieldsAllBrackets.replace("connectedSubfields", "");

						// Get everything between the 2 outermost squarebrackets:
						connectedSubfields = getBracketValues(connectedSubfieldsAllBrackets);
					}	
				}

				if (lstValuesClean.contains("translateConnectedSubfields")) {
					int index = lstValuesClean.indexOf("translateConnectedSubfields");
					String translateConnectedSubfieldsString = lstValues.get(index).trim(); // Get whole string incl. square brackets, e. g. translateConnectedSubfields[translate.properties]					
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					translateConnectedSubfields = true;
					if (translateConnectedSubfieldsString != null) {
						String translateConnectedSubfieldsFilename = null;
						Pattern pattern = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcher = pattern.matcher(translateConnectedSubfieldsString);
						translateConnectedSubfieldsFilename = (matcher.find()) ? matcher.group().replaceFirst("\\[", "").replaceFirst("\\]$", "").trim() : null;
						if (translateConnectedSubfieldsFilename != null) {
							translateConnectedSubfieldsProperties = AkImporterHelper.getTranslateProperties(translateConnectedSubfieldsFilename, pathToTranslationFiles, useDefaultMabProperties);
						}
					}
				}


				if (lstValuesClean.contains("concatenatedSubfields")) {
					String concatenatedSubfieldsString = null;
					int index = lstValuesClean.indexOf("concatenatedSubfields");
					concatenatedSubfieldsString = lstValues.get(index).trim(); // Get whole string incl. square brackets, e. g. concatenatedSubfields[a:b:c:, ]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					hasConcatenatedSubfields = true;

					if (concatenatedSubfieldsString != null) {
						// Extract the text in the square brackets:
						Pattern patternConcatenatedSubfields = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between the first and last squary brackets
						Matcher matcherConcatenatedSubfields = patternConcatenatedSubfields.matcher(concatenatedSubfieldsString);
						String concatenatedSubfieldsAllBrackets = (matcherConcatenatedSubfields.find()) ? matcherConcatenatedSubfields.group().trim() : null;
						concatenatedSubfieldsAllBrackets = concatenatedSubfieldsAllBrackets.replace("concatenatedSubfields", "");
						concatenatedSubfieldsAllBrackets = concatenatedSubfieldsAllBrackets.replace("\\,", ",");

						// Get everything between the 2 outermost squarebrackets:
						concatenatedSubfields = getBracketValues(concatenatedSubfieldsAllBrackets);
						
						// Get applyToFields and add them to a LinkedHashMap<String, List<String>> that we will
						// pass on with the current PropertiesObject to the MatchingOperations.
						List<String> concatenatedSubfieldsApplyToFields = getApplyToFields(concatenatedSubfields);
						if (concatenatedSubfieldsApplyToFields != null) {
							applyToFields.put("concatenatedSubfields", concatenatedSubfieldsApplyToFields);
							concatenatedSubfields = removeApplyToFields(concatenatedSubfields);
							
							// Get separator value:
							List<String> bracketContentAsList = Arrays.asList(concatenatedSubfields.get(1).replace("[", "").replace("]", "").split(":"));
							concatenatedSubfieldsSeparator = bracketContentAsList.get(bracketContentAsList.size()-1); // Get last list element. This should be the separator.
							
						} else {
							// Get separator value:
							List<String> bracketContentAsList = Arrays.asList(concatenatedSubfieldsAllBrackets.replace("[", "").replace("]", "").split(":"));
							concatenatedSubfieldsSeparator = bracketContentAsList.get(bracketContentAsList.size()-1); // Get last list element. This should be the separator.
						}
					}
				}


				if (lstValuesClean.contains("translateConcatenatedSubfields")) {
					int index = lstValuesClean.indexOf("translateConcatenatedSubfields");
					String translateConcatenatedSubfieldsString = lstValues.get(index).trim(); // Get whole string incl. square brackets, e. g. translateConcatenatedSubfields[translate.properties]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					translateConcatenatedSubfields = true;
					if (translateConcatenatedSubfieldsString != null) {
						String translateConcatenatedSubfieldsFilename = null;
						Pattern pattern = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcher = pattern.matcher(translateConcatenatedSubfieldsString);
						translateConcatenatedSubfieldsFilename = (matcher.find()) ? matcher.group().replaceFirst("\\[", "").replaceFirst("\\]$", "").trim() : null;
						if (translateConcatenatedSubfieldsFilename != null) {
							translateConcatenatedSubfieldsProperties = AkImporterHelper.getTranslateProperties(translateConcatenatedSubfieldsFilename, pathToTranslationFiles, useDefaultMabProperties);
						}
					}
				}

				if (lstValuesClean.contains("subfieldExists")) {
					String subfieldExistsString = null;
					int index = lstValuesClean.indexOf("subfieldExists");
					subfieldExistsString = lstValues.get(index).trim(); // Get whole string incl. square brackets, e. g. subfieldExists[a:b:c:AND]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					hasSubfieldExists = true;
					if (subfieldExistsString != null) {
						// Extract the text in the square brackets:
						Pattern patternSubfieldExists = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between the first and last squary brackets
						Matcher matcherSubfieldExists = patternSubfieldExists.matcher(subfieldExistsString);
						String subfieldExistsAllBrackets = (matcherSubfieldExists.find()) ? matcherSubfieldExists.group().trim() : null;
						subfieldExistsAllBrackets = subfieldExistsAllBrackets.replace("subfieldExists", "");

						// Get everything between the 2 outermost squarebrackets:
						subfieldExists = getBracketValues(subfieldExistsAllBrackets);
						
						// Get applyToFields and add them to a LinkedHashMap<String, List<String>> that we will
						// pass on with the current PropertiesObject to the MatchingOperations.
						List<String> subfieldExistsApplyToFields = getApplyToFields(subfieldExists);
						if (subfieldExistsApplyToFields != null) {
							applyToFields.put("subfieldExists", subfieldExistsApplyToFields);
							subfieldExists = removeApplyToFields(subfieldExists);
						}
					}	
				}

				if (lstValuesClean.contains("subfieldNotExists")) {
					String subfieldNotExistsString = null;
					int index = lstValuesClean.indexOf("subfieldNotExists");
					subfieldNotExistsString = lstValues.get(index).trim(); // Get whole string incl. square brackets, e. g. subfieldNotExists[a:b:c:OR]
					lstValues.remove(index); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					hasSubfieldNotExists = true;
					if (subfieldNotExistsString != null) {
						// Extract the text in the square brackets:
						Pattern patternSubfieldNotExists = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between the first and last squary brackets
						Matcher matcherSubfieldNotExists = patternSubfieldNotExists.matcher(subfieldNotExistsString);
						String subfieldNotExistsAllBrackets = (matcherSubfieldNotExists.find()) ? matcherSubfieldNotExists.group().trim() : null;
						subfieldNotExistsAllBrackets = subfieldNotExistsAllBrackets.replace("subfieldNotExists", "");

						// Get everything between the 2 outermost squarebrackets:
						subfieldNotExists = getBracketValues(subfieldNotExistsAllBrackets);
						
						// Get applyToFields and add them to a LinkedHashMap<String, List<String>> that we will
						// pass on with the current PropertiesObject to the MatchingOperations.
						List<String> subfieldNotExistsApplyToFields = getApplyToFields(subfieldNotExists);
						if (subfieldNotExistsApplyToFields != null) {
							applyToFields.put("subfieldNotExists", subfieldNotExistsApplyToFields);
							subfieldNotExists = removeApplyToFields(subfieldNotExists);
						}
					}	
				}
				

				if (lstValuesClean.contains("regEx")) {
					String regexValueString = null;
					hasRegex = true;
					hasRegexStrict = false;
					hasRegExReplace = false;
					int index = lstValuesClean.indexOf("regEx");
					regexValueString =  lstValues.get(index); // Get whole string of regex value incl. square brackets, e. g. regEx[REGEX]
					lstValues.remove(index); // Use index of clean list (without square brackets).
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					if (regexValueString != null) {
						// Extract the regex value in the square brackets:
						Pattern patternRegexValue = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcherRegexValue = patternRegexValue.matcher(regexValueString);
						regexValue = (matcherRegexValue.find()) ? matcherRegexValue.group().replaceFirst("\\[", "").replaceFirst("\\]$", "").replaceAll("\\\\,", ",").trim() : null;
					}
				}

				if (lstValuesClean.contains("regExStrict")) {
					String regexStrictValueString = null;
					hasRegexStrict = true;
					hasRegex = false;
					hasRegExReplace = false;
					int index = lstValuesClean.indexOf("regExStrict");
					regexStrictValueString =  lstValues.get(index); // Get whole string of regex value incl. square brackets, e. g. regExStrict[REGEX]
					lstValues.remove(index); // Use index of clean list (without square brackets).
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.
					if (regexStrictValueString != null) {
						// Extract the regex value in the square brackets:
						Pattern patternRegexStrictValue = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcherRegexStrictValue = patternRegexStrictValue.matcher(regexStrictValueString);
						regexStrictValue = (matcherRegexStrictValue.find()) ? matcherRegexStrictValue.group().replaceFirst("\\[", "").replaceFirst("\\]$", "").replaceAll("\\\\,", ",").trim() : null;
					}
				}

				if (lstValuesClean.contains("regExReplace")) {
					String regExReplaceValueString = null;
					hasRegExReplace = true;
					hasRegexStrict = false;
					hasRegex = false;
					int index = lstValuesClean.indexOf("regExReplace");
					regExReplaceValueString =  lstValues.get(index); // Get whole string of regex value incl. square brackets, e. g. regExStrict[REGEX]
					lstValues.remove(index); // Use index of clean list (without square brackets).
					lstValuesClean.remove(index); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations.

					if (regExReplaceValueString != null) {
						// Extract the regex value in the square brackets:
						Pattern patternRegexReplaceValue = java.util.regex.Pattern.compile("\\[.*?\\]$"); // Get everything between square brackets and the brackets themselve (we will remove them later)
						Matcher matcherRegexReplaceValue = patternRegexReplaceValue.matcher(regExReplaceValueString);
						regexReplaceValue = (matcherRegexReplaceValue.find()) ? matcherRegexReplaceValue.group().trim() : null;
					}
					regexReplaceValue = regexReplaceValue.replace("regExReplace", "");

					// Get everything between the 2 outermost squarebrackets:
					regexReplaceValues = getBracketValues(regexReplaceValue);
				}


				if (lstValuesClean.contains("allowDuplicates")) {
					allowDuplicates = true;
					lstValues.remove(lstValuesClean.indexOf("allowDuplicates")); // Use index of clean list (without square brackets). Problem is: We can't use regex in "indexOf".
					lstValuesClean.remove(lstValuesClean.indexOf("allowDuplicates")); // Remove value also from clean list so that we always have the same no. of list elements (and thus the same value for "indexOf") for later operations. 
				}


				// Get all multiValued fields and remove them after we finished:
				if (multiValued) {
					for(String lstValue : lstValues) {

						// Remove  square brackets from the mabfield name (in case it is also a translateValue field) so that
						// we have a clear mabfield-name to process. If not, it wouldn't work in the matching operations!
						String cleanLstValue = lstValue.replaceAll("\\[.*?\\]", "");
						mabFieldnames.put(cleanLstValue, null);

						// Do not remove field if it's also a translateValue, translateValueContains or translateValueRegex (then just add "null"),
						// because in this case we will still need the fields further down for the translate operations.
						fieldsToRemove.add((translateValue || translateValueContains || translateValueRegex) ? null : lstValue);
					}
					lstValues.removeAll(fieldsToRemove);
					fieldsToRemove.clear();
				}

				// Get all customText fields and remove them after we finished:
				if (customText) {
					for(String lstValue : lstValues) {
						ArrayList<String> solrFieldvalue = new ArrayList<String>();
						solrFieldvalue.add(lstValue);
						customTextFields.add(new SolrField(key, solrFieldvalue, multiValued, allowDuplicates));
						fieldsToRemove.add(lstValue);
					}
					lstValues.removeAll(fieldsToRemove);
					fieldsToRemove.clear();
				}				

				// Get all translateValue, translateValueContains and translateValueRegex fields and remove them after we finished:
				if (translateValue || translateValueContains || translateValueRegex) {

					if (filename != null) {

						// Get the mapping values from .properties file:
						translateProperties = AkImporterHelper.getTranslateProperties(filename, pathToTranslationFiles, useDefaultMabProperties);

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
						System.err.println("Error: You need to specify a translation-properties file with the file-ending \".properties\"!");
					}
				}
				
				// Set applyToField to null if no applyToField exists. This is just to avoid overhead.
				if (applyToFields.isEmpty()) {
					applyToFields = null;
				}

				// Create Leader, Datafield and Controlfied objects for all fields given in mab.properties
				for(String lstValueClean : lstValuesClean) {
					if (lstValueClean.length() == 3) { // It's a controlfield (it has only 3 characters, e. g. SYS)
						Controlfield controlfield = new Controlfield();
						controlfield.setTag(lstValueClean);
						controlfields.add(controlfield);
					} else if (lstValueClean.length() == 8) { // It should be a datafield (it has 8 characters, e. g. 100$**$a)
						Datafield datafield = new Datafield();
						Subfield subfield = new Subfield();
						ArrayList<Subfield> subfields = new ArrayList<Subfield>();
						String tag = lstValueClean.substring(0, 3);
						String ind1 = lstValueClean.substring(4, 5);
						String ind2 = lstValueClean.substring(5, 6);
						String subfieldCode = lstValueClean.substring(7, 8);
						subfield.setCode(subfieldCode);
						subfields.add(subfield);
						datafield.setTag(tag);
						datafield.setInd1(ind1);
						datafield.setInd2(ind2);
						datafield.setSubfields(subfields);
						datafields.add(datafield);
					} else if (lstValueClean.equals("leader")) { // This is the leader tag
						leader = new Leader();
						leader.setTag("leader");
					}
				}


				// Get all default fields (the other fields were removed):
				for(String lstValue : lstValues) {
					mabFieldnames.put(lstValue, null);
					fieldsToRemove.add(lstValue);
				}
				lstValues.removeAll(fieldsToRemove);
				fieldsToRemove.clear();

				if (!customText) { // Ignore custom texts because they don't have to be treated in MatchingOperations
					PropertiesObject mo = new PropertiesObject(
							key,
							mabFieldnames,
							leader,
							datafields,
							controlfields,
							multiValued,
							customText,
							getAllFields,
							allFieldsExceptions,
							getFullRecordAsXML,
							translateValue,
							translateValueContains,
							translateValueRegex,
							translateProperties,
							hasDefaultValue,
							defaultValue,
							hasConnectedSubfields,
							connectedSubfields,
							translateConnectedSubfields,
							translateConnectedSubfieldsProperties,
							hasConcatenatedSubfields,
							concatenatedSubfields,
							concatenatedSubfieldsSeparator,
							translateConcatenatedSubfields,
							translateConcatenatedSubfieldsProperties,
							hasRegex,
							regexValue,
							hasRegexStrict,
							regexStrictValue,
							hasRegExReplace,
							regexReplaceValues,
							allowDuplicates,
							hasSubfieldExists,
							subfieldExists,
							hasSubfieldNotExists,
							subfieldNotExists,
							applyToFields
							);

					propertiesObjects.add(mo);
				}
			}

		} catch (IOException e) {
			propertiesObjects = null;			
			System.err.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.err.println("IOException!");
			System.err.println("\nSee also StackTrace:\n");
			e.printStackTrace();
			System.err.println("\n-----------------------------------------------------------------------\n");
		}


		return propertiesObjects;
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
	/*
	private HashMap<String, String> getTranslateProperties(String filename, String pathToTranslationFiles) {

		HashMap<String, String> translateProperties = new HashMap<String, String>();

		Properties properties = new Properties();
		String translationFile = pathToTranslationFiles + File.separator + filename;
		BufferedInputStream translationStream = null;

		try {
			// Get .properties file and load contents:
			if (useDefaultMabProperties) {
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
	*/


	/**
	 * Getting values between square brackets as Map.
	 * 
	 * @param rawValue			String: The raw value with square brackets, e. g. connectedSubfields[b:4:NoRole][9:NoGndId]
	 * @return					Map<Integer, String>: Integer indicates the position of the square bracket, String is the value within the bracket
	 */
	private LinkedHashMap<Integer, String> getBracketValues(String rawValue) {		
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
	 * Get the applyToFields option from the bracket values.
	 * 
	 * @param bracketValues		LinkedHashMap<Integer, String>: The bracket values from whicht the applyToFields option should be gotten
	 * @return					List<String>: A list of the applyToFields
	 */
	private List<String> getApplyToFields(LinkedHashMap<Integer, String> bracketValues) {
		List<String> applyToFields = new ArrayList<String>();
		
		for (Entry<Integer, String> bracketValueEntry : bracketValues.entrySet()) {
			String bracketValue = bracketValueEntry.getValue();
			
			if (bracketValue.startsWith("applyToFields[")) {
				String applyToFieldsAsString = getBracketValues(bracketValue).get(1);
				applyToFields = Arrays.asList(applyToFieldsAsString.split("\\s*:\\s*")); // "applyToFields" as List<String>
			} else {
				applyToFields = null; // option "applyToFields" is not specified
			}
		}
		return applyToFields;
	}
	
	
	/**
	 * Removes the "applyToFields" option from bracket values.
	 * @param bracketValues		LinkedHashMap<Integer, String>: The bracket values from which the applyToFields option should be removed.
	 * @return					LinkedHashMap<Integer, String>: Bracket values without applyToFields option
	 */
	private LinkedHashMap<Integer, String> removeApplyToFields(LinkedHashMap<Integer, String> bracketValues) {
		for (Entry<Integer, String> bracketValueEntry : bracketValues.entrySet()) {
			String bracketValue = bracketValueEntry.getValue();
			int bracketKey = bracketValueEntry.getKey();
			if (bracketValue.startsWith("applyToFields[")) {
				bracketValues.remove(bracketKey);
			}
		}
		return bracketValues;
	}

}