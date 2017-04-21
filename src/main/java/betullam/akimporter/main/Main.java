/**
 * This is the main file of AkImporter that is started when the programm is invoked.
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
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.net.MalformedServerReplyException;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPFileFilter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;

import main.java.betullam.akimporter.akindex.AkIndex;
import main.java.betullam.akimporter.akindex.AkIndexAllFields;
import main.java.betullam.akimporter.solrmab.PostProcess;
import main.java.betullam.akimporter.solrmab.PostProcessor;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.XmlIndex;
import main.java.betullam.akimporter.updater.OaiUpdater;
import main.java.betullam.akimporter.updater.Updater;

/** TODO:
 * 
 * 1. Tidy up code
 * 2. Better error messages in try-catch blocks.
 * 3. Better logging (Log4J2 or Slf4J).
 * 4. Translation console output to german?
 *
 */


public class Main {

	// General
	public static String pathToAkImporterJar = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath(); // Path to AkImporter.jar
	public static String akImporterExecutionPath = new File(pathToAkImporterJar).getParent(); // AkImporter.jar execution path
	static boolean optimize = false;
	static boolean print = false;
	static boolean test = false;
	static boolean flag = false;
	static boolean merge = false;
	static boolean isUpdateSuccessful = false;

	// CLI options
	static Options options = new Options();
	static OptionGroup optionGroup = new OptionGroup();

	// Get settings from AkImporter.properties file and set them to variables
	static Properties importerProperties = getImporterProperties(akImporterExecutionPath + File.separator + "AkImporter.properties");
	static String iDataset = importerProperties.getProperty("import.dataset");
	static String iPath = importerProperties.getProperty("import.path");
	static boolean iValidation = (importerProperties.getProperty("import.validation") != null && importerProperties.getProperty("import.validation").equals("V")) ? true : false;
	static String iSolr = importerProperties.getProperty("import.solr");
	static boolean iDefaultMabProperties = (importerProperties.getProperty("import.defaultMabProperties") != null && importerProperties.getProperty("import.defaultMabProperties").equals("D")) ? true : false;
	static String iCustomMabProperties = importerProperties.getProperty("import.customMabProperties");
	static String uFtpHost = importerProperties.getProperty("update.ftpHost");
	static String uFtpPortStr = importerProperties.getProperty("update.ftpPort");
	static int uFtpPort = (uFtpPortStr != null && !uFtpPortStr.isEmpty() && uFtpPortStr.matches("^\\d+$")) ? Integer.valueOf(importerProperties.getProperty("update.ftpPort")) : 21;
	static String uFtpUser = importerProperties.getProperty("update.ftpUser");
	static String uFtpPass = importerProperties.getProperty("update.ftpPass");
	static String uRemotePath = importerProperties.getProperty("update.remotePath");
	static String uLocalPath = importerProperties.getProperty("update.localPath");
	static String uSolr = importerProperties.getProperty("update.solr");
	static boolean uDefaultMabProperties = (importerProperties.getProperty("update.defaultMabProperties") != null && importerProperties.getProperty("update.defaultMabProperties").equals("D")) ? true : false;
	static String uCustomMabProperties = importerProperties.getProperty("update.customMabProperties");
	static String aPath = importerProperties.getProperty("authority.path");
	static String aSolrAuth = importerProperties.getProperty("authority.solrAuth");
	static String aSolrBibl = importerProperties.getProperty("authority.solrBibl");
	static boolean aDefaultMabProperties = (importerProperties.getProperty("authority.defaultMabProperties") != null && importerProperties.getProperty("authority.defaultMabProperties").equals("D")) ? true : false;
	static String aCustomMabProperties = importerProperties.getProperty("authority.customMabProperties");
	static String aUpdateLastUpdateFile = importerProperties.getProperty("authority.update.lastUpdateFile");
	static String aUpdateLocalPath = importerProperties.getProperty("authority.update.localPath");
	static String aUpdateOaiUrl = importerProperties.getProperty("authority.update.oaiUrl");
	static String aUpdateFormat = importerProperties.getProperty("authority.update.format");
	static List<String> aUpdateOaiSets = (importerProperties.getProperty("authority.update.set") != null) ? Arrays.asList(importerProperties.getProperty("authority.update.set").split("\\s*,\\s*")) : null;
	static String aMergeEntities = importerProperties.getProperty("authority.merge.entities");


	/**
	 * Main method of AkImporter.
	 * This is the starting point for all following actions.
	 * 
	 * @param	args	Command line arguments
	 */
	public static void main(String[] args) {

		// Disable StatusLogger message of Log4J2:
		StatusLogger.getLogger().setLevel(Level.OFF);

		// Set the command line options:
		CommandLineParser clParser = new DefaultParser();
		setCLI();

		if (args.length <= 0) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp("AkImporter", "", options, "", true);
			return;
		}

		try {

			CommandLine cmd = clParser.parse(options, args, true);
			String selectedMainOption = optionGroup.getSelected();

			// Verbose?
			if (cmd.hasOption("v")) {
				print = true;
			}

			// Optimize?
			if (cmd.hasOption("o")) {
				optimize = true;
			}

			// Test?
			if (cmd.hasOption("t")) {
				test = true;
			}

			// Flag only?
			if (cmd.hasOption("f")) {
				flag = true;
			}

			// Merge authority data?
			if (cmd.hasOption("m")) {
				merge = true;
			}

			// Get OAI import properties. We need to get them here because we need the "cmd" variable for it.
			String oaiName = null;
			if (cmd.hasOption("O")) {
				oaiName = cmd.getOptionValue("O");
			}
			if (cmd.hasOption("oai_reimport")) {
				oaiName = cmd.getOptionValue("oai_reimport");
			}
			String oaiUrl = importerProperties.getProperty("oai." + oaiName + ".url");
			String format = importerProperties.getProperty("oai." + oaiName + ".format");
			List<String> sets = (importerProperties.getProperty("oai." + oaiName + ".set") != null) ? Arrays.asList(importerProperties.getProperty("oai." + oaiName + ".set").split("\\s*,\\s*")) : null;
			String destinationPath = importerProperties.getProperty("oai." + oaiName + ".destinationPath");
			String oaiDatefile = importerProperties.getProperty("oai." + oaiName + ".dateFile");
			String oaiPropertiesFile = importerProperties.getProperty("oai." + oaiName + ".propertiesFile");
			String solrServerBiblio = importerProperties.getProperty("oai." + oaiName + ".solrBibl");
			String elementsToMerge = importerProperties.getProperty("oai." + oaiName + ".elements");
			//System.out.println("Elements Level for " + oaiName + ": " + importerProperties.getProperty("oai." + oaiName + ".elementsLevel"));
			//if (importerProperties.getProperty("oai." + oaiName + ".elementsLevel") != null) { System.out.println ("Is NOT Null"); } else { System.out.println ("Is Null"); };
			int elementsToMergeLevel = (importerProperties.getProperty("oai." + oaiName + ".elementsLevel") != null) ? Integer.valueOf(importerProperties.getProperty("oai." + oaiName + ".elementsLevel")) : 1;
			List<String> structElements = (importerProperties.getProperty("oai." + oaiName + ".structElements") != null) ? Arrays.asList(importerProperties.getProperty("oai." + oaiName + ".structElements").split("\\s*,\\s*")) : null;
			List<String> include = (importerProperties.getProperty("oai." + oaiName + ".include") != null) ? Arrays.asList(importerProperties.getProperty("oai." + oaiName + ".include").split("\\s*,\\s*")) : null;
			List<String> exclude = (importerProperties.getProperty("oai." + oaiName + ".exclude") != null) ? Arrays.asList(importerProperties.getProperty("oai." + oaiName + ".exclude").split("\\s*,\\s*")) : null;
			String deleteBeforeImport = importerProperties.getProperty("oai." + oaiName + ".deleteBeforeImport");
			boolean deleteOldLocalFiles = (importerProperties.getProperty("oai." + oaiName + ".deleteOldLocal") != null) ? Boolean.valueOf(importerProperties.getProperty("oai." + oaiName + ".deleteOldLocal")) : false;


			// Get XML import properties. We need to get them here because we need the "cmd" variable for it.
			String xmlName = null;
			if (cmd.hasOption("X")) {
				xmlName = cmd.getOptionValue("X");
			}
			String xmlPath = importerProperties.getProperty("xml." + xmlName + ".path");
			String xmlPropertiesFile = importerProperties.getProperty("xml." + xmlName + ".propertiesFile");
			String xmlSolrServerBiblio = importerProperties.getProperty("xml." + xmlName + ".solrBibl");
			String xmlElements = importerProperties.getProperty("xml." + xmlName + ".elements");
			List<String> xmlInclude = (importerProperties.getProperty("xml." + xmlName + ".include") != null) ? Arrays.asList(importerProperties.getProperty("xml." + xmlName + ".include").split("\\s*,\\s*")) : null;
			List<String> xmlExclude = (importerProperties.getProperty("xml." + xmlName + ".exclude") != null) ? Arrays.asList(importerProperties.getProperty("xml." + xmlName + ".exclude").split("\\s*,\\s*")) : null;
			String xmlDeleteBeforeImport = importerProperties.getProperty("xml." + xmlName + ".deleteBeforeImport");
			/*String xmlFtpHost = importerProperties.getProperty("xml." + xmlName + ".ftpHost");
			String xmlFtpPort = importerProperties.getProperty("xml." + xmlName + ".ftpPort");
			String xmlFtpUser = importerProperties.getProperty("xml." + xmlName + ".ftpUser");
			String xmlFtpPass = importerProperties.getProperty("xml." + xmlName + ".ftpPass");
			String xmlFtpRemotePath = importerProperties.getProperty("xml." + xmlName + ".ftpRemotePath");
			String xmlFtpLocalPath = importerProperties.getProperty("xml." + xmlName + ".ftpLocalPath");*/

			// Get AKindex properties. We need to get them here because we need the "cmd" variable for it.
			String akiName = null;
			if (cmd.hasOption("ak_index")) {
				akiName = cmd.getOptionValue("ak_index");
			}

			// Switch between main options
			switch (selectedMainOption) {

			case "k": { // FOR TESTING ONLY
				System.out.println("No test case specified. DON'T EVER USE THIS COMMAND IN PRODUCTION ENVIRONMENT!");				
				break;
			}

			case "i": {
				new Import(optimize, print);
				postProcess();
				break;
			}

			case "p": {

				// Show properties and ask user for confirmation:
				Scanner scanner = new Scanner(System.in);
				System.out.println("Your import settings in AkImporter.properties are:");
				System.out.println("-------------------------------------------");
				for (String key : importerProperties.stringPropertyNames()) {
					if (key.startsWith("import.")) {
						System.out.println(key + ": " + importerProperties.getProperty(key));
					}
				}
				System.out.println("-------------------------------------------");
				String startImport = getUserInput("Would you like to start the import process with these settings?\n Y = Yes, start import\n N = No, cancel import", "Y, N", scanner);

				if (startImport.equals("Y")) {
					if(checkImportProperties()) {
						if (test) {
							// If test option is specified, just tell the user if the properties are OK, but do not start the process
							System.out.println("Properties are OK");
							break;
						} else {
							// If the properties are OK, start the import process:
							new Import(
									iDataset,
									iPath,
									iPath,
									iValidation,
									iSolr,
									iDefaultMabProperties,
									iCustomMabProperties,
									optimize,
									print
									);
							postProcess();
						}
					}
				} else if (startImport.equals("N")){
					System.out.println("\nImport process cancelled as requested by user.\n");
				}

				break;
			}

			case "P": {

				if(checkImportProperties()) {
					if (test) {
						// If test option is specified, just tell the user if the properties are OK, but do not start the process
						System.out.println("Properties are OK");
						break;
					} else {
						// If the properties are OK, start the import process with settings in AkImporter.properties immediately:
						new Import(
								iDataset,
								iPath,
								iPath,
								iValidation,
								iSolr,
								iDefaultMabProperties,
								iCustomMabProperties,
								optimize,
								print
								);
						postProcess();
					}
				}

				break;
			}

			case "r": {
				boolean doNotCheckFtp = false;
				if(checkImportProperties() && checkUpdateProperties(doNotCheckFtp)) {
					if (test) {
						// If test option is specified, just tell the user if the properties are OK, but do not start the process
						System.out.println("Properties are OK");
						break;
					} else {
						System.out.println("\n-----------------------------------\n");
						System.out.println("Start re-importing initial dataset ...");

						// Start import process of initial dataset:
						new Import(
								iDataset,
								iPath,
								iPath,
								iValidation,
								iSolr,
								iDefaultMabProperties,
								iCustomMabProperties,
								optimize,
								print
								);
						System.out.println("\n-----------------------------------\n");


						// Start import process of ongoing updates (from "merged data" directory):
						System.out.println("Start re-importing ongoing data updates ...");
						ReImport reImport = new ReImport(print, optimize);
						reImport.reImportOngoing(
								uLocalPath,
								iValidation,
								uSolr,
								uDefaultMabProperties,
								uCustomMabProperties
								);
						boolean isReImportingSuccessful = reImport.isReImportingSuccessful();
						if (isReImportingSuccessful) {
							System.out.println("\n-----------------------------------\n");
							System.out.println("Re-Importing of all data was successful.");
							postProcess();
						} else {
							System.err.println("Error while re-importing of ongoing data updates.");
						}
					}
				}

				break;
			}

			case "R": {
				boolean doNotCheckFtp = false;
				if(checkImportProperties() && checkUpdateProperties(doNotCheckFtp)) {
					if (test) {
						// If test option is specified, just tell the user if the properties are OK, but do not start the process
						System.out.println("Properties are OK");
						break;
					} else {
						System.out.println("Start re-importing ongoing data updates ...");
						// Start import process of ongoing updates (from "merged data" directory):				
						ReImport reImport = new ReImport(print, optimize);
						reImport.reImportOngoing(
								uLocalPath,
								iValidation,
								uSolr,
								uDefaultMabProperties,
								uCustomMabProperties
								);
						postProcess();
					}
				}

				break;
			}

			case "l": {
				// Connect child and parent volumes:	
				HttpSolrServer solrServer = new HttpSolrServer(iSolr);
				Relate relate = new Relate(solrServer, null, optimize, print);
				boolean isRelateSuccessful = relate.isRelateSuccessful();

				if (isRelateSuccessful) {
					if (cmd.hasOption("v")) {
						System.out.println("Done linking parent and child records. Everything was successful.");
					}
					postProcess();
				}
				break;
			}

			case "post_process": {
				postProcess();
				break;
			}

			case "u": {

				// Check if update properties are correct
				if(checkUpdateProperties(true)) {
					if (test) {
						// If test option is specified, just tell the user if the properties are OK, but do not start the process
						System.out.println("Properties are OK");
						break;
					} else {
						// If the properties are OK, start the update process:
						Updater updater = new Updater();																			
						isUpdateSuccessful = updater.update (
								uRemotePath,
								uLocalPath,
								uFtpHost,
								uFtpPort,
								uFtpUser,
								uFtpPass,
								uSolr,
								aSolrAuth,
								uDefaultMabProperties,
								uCustomMabProperties,
								aMergeEntities,
								flag,
								merge,
								optimize,
								print
								);
						postProcess();
					}
				}

				break;
			}

			case "a": {
				// Show properties for authority importing and ask user for confirmation:
				Scanner scanner = new Scanner(System.in);
				System.out.println("Your authority settings in AkImporter.properties are:");
				System.out.println("-------------------------------------------");
				for (String key : importerProperties.stringPropertyNames()) {
					if (key.startsWith("authority.")) {
						System.out.println(key + ": " + importerProperties.getProperty(key));
					}
				}
				System.out.println("-------------------------------------------");
				String startImport = getUserInput("Would you like to start the import process of the authority file with these settings?\n Y = Yes, start import\n N = No, cancel import", "Y, N", scanner); 

				if (startImport.equals("Y")) {
					if(checkAuthorityProperties()) {
						if (test) {
							// If test option is specified, tell the user if the properties are OK, but do not start the process
							if (merge) {
								boolean mergeEntitiesOk = (aMergeEntities != null && !aMergeEntities.isEmpty()) ? true : false;
								if (mergeEntitiesOk) {
									System.out.println("Properties are OK");
								}
							} else {
								System.out.println("Properties are OK");
							}
							break;
						} else {
							// Start import process for authority records:
							Authority auth = new Authority(
									flag,
									merge,
									aMergeEntities,
									aPath,
									aDefaultMabProperties,
									aCustomMabProperties,
									aSolrAuth,
									aSolrBibl,
									null,
									print,
									optimize
									);
							auth.indexAuthority();
						}
					}

				} else if (startImport.equals("N")){
					System.out.println("\nImport process cancelled as requested by user.\n");
				}

				break;
			}

			case "A": {
				if(checkAuthorityProperties()) {
					if (test) {
						// If test option is specified, tell the user if the properties are OK, but do not start the process
						if (merge) {
							boolean mergeEntitiesOk = (aMergeEntities != null && !aMergeEntities.isEmpty()) ? true : false;
							if (mergeEntitiesOk) {
								System.out.println("Properties are OK");
							}
						} else {
							System.out.println("Properties are OK");
						}
						break;
					} else {
						// Start import process for authority records:
						Authority auth = new Authority(
								flag,
								merge,
								aMergeEntities,
								aPath,
								aDefaultMabProperties,
								aCustomMabProperties,
								aSolrAuth,
								aSolrBibl,
								null,
								print,
								optimize
								);
						auth.indexAuthority();
					}
				}

				break;
			}

			case "e": {
				if(checkAuthorityUpdateProperties()) {
					if (test) {
						// If test option is specified, just tell the user if the properties are OK, but do not start the process
						System.out.println("Properties are OK");
						break;
					} else {
						OaiUpdater oaiUpdater = new OaiUpdater();

						oaiUpdater.oaiGndUpdate(
								aUpdateOaiUrl,
								aUpdateFormat,
								aUpdateOaiSets,
								aUpdateLocalPath,
								aUpdateLastUpdateFile,
								aDefaultMabProperties,
								aCustomMabProperties,
								aSolrAuth,
								aSolrBibl,
								aMergeEntities,
								merge,
								print,
								optimize
								);						
					}
				}
				break;
			}

			case "c": {
				String consolidatedFile = new File(iPath).getParent() + File.separator + "consolidated.xml";
				new Consolidate(
						iPath,
						uLocalPath,
						consolidatedFile,
						true
						);
				break;
			}

			case "O": {

				System.out.println("Starting OAI harvesting for " + oaiName + " ...");				

				OaiUpdater oaiUpdater = new OaiUpdater();
				try {
					oaiUpdater.oaiGenericUpdate(
							oaiUrl,
							format,
							sets,
							structElements,
							destinationPath,
							elementsToMerge,
							elementsToMergeLevel,
							oaiDatefile,
							include,
							exclude,
							deleteBeforeImport,
							deleteOldLocalFiles,
							oaiPropertiesFile,
							solrServerBiblio,
							print,
							optimize);
					postProcess();
				} catch (main.java.betullam.akimporter.updater.OaiUpdater.ValidatorException e) {
					e.printStackTrace();
				}

				break;
			}

			case "oai_reimport": {

				AkImporterHelper.print(print, "Start reimporting OAI data for " + oaiName + " ...");

				OaiUpdater oaiUpdater = new OaiUpdater();
				try {
					oaiUpdater.reImportOaiData(
							destinationPath,
							false,
							format,
							solrServerBiblio,
							structElements,
							elementsToMerge,
							include,
							exclude,
							deleteBeforeImport,
							deleteOldLocalFiles,
							oaiPropertiesFile,
							optimize,
							print);
					postProcess();
				} catch (main.java.betullam.akimporter.updater.OaiUpdater.ValidatorException e) {
					e.printStackTrace();
				}
				AkImporterHelper.print(print, "\nDone reimporting OAI data for " + oaiName + ".");
				break;
			}

			case "X": {

				System.out.println("Starting XML import for " + xmlName + " ...");
				new XmlIndex(xmlPath, xmlPropertiesFile, xmlSolrServerBiblio, xmlElements, xmlInclude, xmlExclude, xmlDeleteBeforeImport, print, optimize);
				System.out.println("Done importing XML for " + xmlName + ".");
				postProcess();
				break;
			}

			case "h": {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.printHelp("AkImporter", "", options, "", true);
				break;
			}

			case "index_sampledata": {
				new Import(true, iSolr, iDefaultMabProperties, iCustomMabProperties);
				break;
			}
			
			case "ak_index": {	
				String akiSolr = importerProperties.getProperty("akindex.setting.solr");
				boolean akiValidateSkip = (importerProperties.getProperty("akindex.setting.validate.skip") != null && !importerProperties.getProperty("akindex.setting.validate.skip").isEmpty()) ? Boolean.valueOf(importerProperties.getProperty("akindex.setting.validate.skip")) : false;
				String akiPath = importerProperties.getProperty("akindex." + akiName + ".path.data");
				String akiAllFieldsPath = importerProperties.getProperty("akindex.setting.path.allfields");
				String akiElements = importerProperties.getProperty("akindex." + akiName + ".elements");
				String akiElementsLevel = importerProperties.getProperty("akindex." + akiName + ".elements.level");
				String akiIdXpath = importerProperties.getProperty("akindex." + akiName + ".xpath.id");
				
				new AkIndex(akiSolr, akiPath, akiElements, akiElementsLevel, akiIdXpath, akiValidateSkip, print, optimize);
				new AkIndexAllFields(akiSolr, akiAllFieldsPath, print);
				break;
			}
			
			case "ak_index_allfields": {
				String akiSolr = importerProperties.getProperty("akindex.setting.solr");
				String akiAllFieldsPath = importerProperties.getProperty("akindex.setting.path.allfields");

				new AkIndexAllFields(akiSolr, akiAllFieldsPath, print);
				break;
			}
			
			default: {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.printHelp("AkImporter", "", options, "", true);
				break;
			}

			}


		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}

		if (print) {
			System.out.print("\n");
		}
	}




	/**
	 * Ask for user input.
	 * You can define a comma-separated String as possible anwers. If the user-input is not one of them, the questions will be asked again until a
	 * correct answer is given. If you ask for a path or file and want to check if it exists, use "fileExists" or "directoryExists" for possibleAnswers.
	 * If you want to check if a .properties file exists, use "propertiesExists" for possibleAnswers. If you want to create a new file, use "newFile" for
	 * possible Answers. You can also ask for a Solr server URL and ping it (check if it is running) by using "solrPing" for possibleAnswers.
	 * If you don't need to validate an answer, use null.
	 * 
	 * @param	question			A question you want the user to answer as a String.
	 * @param	possibleAnswers		A comma-separated String with possible answers: "fileExists", "directoryExists", "propertiesExists", "newFile", "solrPing" or null (see above explanation).
	 * @param	scanner				A Scanner(System.in) object.
	 * @return	String				The value the user specified
	 */
	public static String getUserInput(String question, String possibleAnswers, Scanner scanner) {
		boolean isValidAnswer = false;
		String userinput = "";
		List<String> lstPossibleAnswers = new ArrayList<String>();

		System.out.println(question);

		while (isValidAnswer == false) {
			userinput = scanner.nextLine();
			File file;
			if (possibleAnswers == null) {
				isValidAnswer = true;
			} else {
				if (possibleAnswers.equals("fileExists")) { // Check if FILE exists
					file = new File(userinput);
					if (fileExists(file)) {
						if (canReadFile(file)) {
							isValidAnswer = true;
						} else {
							isValidAnswer = false;
							System.out.println("Keine Zugriffsberechtigung auf Datei! Bitte Datei mit Zugriffsberechtigung angeben oder Importer mit su-Rechten erneut starten!");
						}
					} else {
						isValidAnswer = false;
						System.out.println("Datei existiert nicht! Bitte erneut eingeben!");
					}
				} else if (possibleAnswers.equals("directoryExists")) { // Check if DIRECTORY exists
					file = new File(userinput);
					if (directoryExists(file)) {
						if (canReadFile(file)) {
							isValidAnswer = true;
						} else {
							isValidAnswer = false;
							System.out.println("Keine Zugriffsberechtigung auf Ordner! Bitte Ordner mit Zugriffsberechtigung angeben oder Importer mit su-Rechten erneut starten!");
						}
					} else {
						isValidAnswer = false;
						System.out.println("Ordner existiert nicht! Bitte erneut eingeben!");
					}
				} else if (possibleAnswers.equals("newFile")) { // Check if a new file can be created
					file = new File(userinput);
					if (canWriteToDirectory(file)) {
						if (fileExists(file)) {
							isValidAnswer = false;
							System.out.println("Diese Datei existiert bereits. Bitte anderen Dateinamen wählen!");
						} else {
							isValidAnswer = true;
						}
					} else {
						isValidAnswer = false;
						System.out.println("Keine Schreibberechtigung für den angegebenen Ort. Bitte anderen Ort wählen!");
					}

				} else if (possibleAnswers.equals("solrPing")) { // Check if Solr server is running
					if (isSolrserverRunning(userinput)) {
						isValidAnswer = true;
					} else {
						isValidAnswer = false;
						System.out.println("Solr-Server ist unter \"" + userinput + "\" nicht erreichbar! Bitte prüfen Sie ob der Server läuft und der angegebene Core existiert und geben Sie die URL erneut ein!");
					}
				} else if (possibleAnswers.equals("propertiesExists")) {
					file = new File(userinput);
					if (fileExists(file) && userinput.endsWith(".properties")) {
						if (canReadFile(file)) {
							isValidAnswer = true;
						} else {
							isValidAnswer = false;
							System.out.println("Keine Zugriffsberechtigung auf .properties-Datei! Bitte .properties-Datei mit Zugriffsberechtigung angeben oder Importer mit su-Rechten erneut starten!");
						}
					} else {
						isValidAnswer = false;
						System.out.println("Datei existiert nicht bzw. ist keine .properties-Datei! Bitte erneut eingeben!");
					}
				} else {
					lstPossibleAnswers = getPossibleAnswers(possibleAnswers);
					if (lstPossibleAnswers.contains(userinput)) {
						isValidAnswer = true;
					} else {
						isValidAnswer = false;
						System.out.println("Falsche Eingabe: " + userinput + " (mögliche Eingaben sind: " + lstPossibleAnswers.toString() + ")");
					}
				}
			}
		}

		return userinput.trim();
	}


	/**
	 * Gets a List<String> of all answers that are possible.
	 * Usefull to show possible input values if user inputs a wrong value.
	 * 
	 * @param 	possibleAnswers	Comma separated String of possible answers
	 * @return	List<String>	All possible answers as List<String>
	 */
	public static List<String> getPossibleAnswers(String possibleAnswers) {
		List<String> lstAnswers = new ArrayList<String>();

		String[] arrPossibleAnswers = possibleAnswers.split(",");

		for (String possibleAnswer : arrPossibleAnswers) {
			lstAnswers.add(possibleAnswer.trim());
		}

		return lstAnswers;
	}


	/**
	 * Check if a file (not directory) exists
	 * 
	 * @param	f		A File object
	 * @return	boolean	true if the file exists
	 */
	public static boolean fileExists(File f) {
		boolean fileExists = false;
		fileExists = (f.exists() && f.isFile()) ? true : false;
		return fileExists;
	}

	/**
	 * Check if a directory (not file) exists
	 * 
	 * @param	f		A File object
	 * @return	boolean	true if the directory exists
	 */
	public static boolean directoryExists(File f) {
		boolean directoryExists = false;
		directoryExists = (f.exists() && f.isDirectory()) ? true : false;
		return directoryExists;
	}

	/**
	 * Check if a file is readable
	 * 
	 * @param	f		A File object
	 * @return	boolean	true if the file is readable
	 */
	public static boolean canReadFile(File f) {
		boolean canRead = false;
		canRead = (f.canRead()) ? true : false;
		return canRead;
	}

	/**
	 * Check if a directory is writeable
	 * 
	 * @param	f		A File object
	 * @return	boolean	true if the directory is writeable
	 */
	public static boolean canWriteToDirectory(File f) {
		boolean canWrite = false;
		f = new File(f.getParent());
		canWrite = (f.canWrite()) ? true : false;
		return canWrite;
	}

	/**
	 * Check if a Solr instance is running at a given URL
	 * 
	 * @param	solrServerUrl	String of a Solr server URL incl. core name, e. g. http://my.solr:8080/corename
	 * @return	boolean			true if Solr is running
	 */
	public static boolean isSolrserverRunning(String solrServerUrl) {
		boolean isSolrserverRunning = false;
		HttpSolrServer solrServer = new HttpSolrServer(solrServerUrl);
		int solrStatus;
		try {
			solrStatus = solrServer.ping().getStatus();
			if (solrStatus == 0) {
				isSolrserverRunning = true;
			}
		} catch (RemoteSolrException e) {
			return isSolrserverRunning;
		} catch (SolrServerException e) {
			return isSolrserverRunning;
		} catch (IOException e) {
			return isSolrserverRunning;
		} catch (IllegalStateException e) {
			return isSolrserverRunning;
		} catch (Exception e) {
			return isSolrserverRunning;
		}

		return isSolrserverRunning;
	}

	/**
	 * Get properties from AkImporter.properties file. This file must exist in the same directory as AkImporter-vX.X.jar
	 * @param 		String that indicates the path to the AkImporter.properties file
	 * @return		Properties object with all properties
	 */
	public static Properties getImporterProperties(String pathToImporterProperties) {

		Properties importerProperties = new Properties();
		FileInputStream propertiesInputStream = null;
		try {
			propertiesInputStream = new FileInputStream(pathToImporterProperties);
			importerProperties.load(new InputStreamReader(propertiesInputStream, "UTF-8"));
			propertiesInputStream.close();
		} catch (FileNotFoundException e) {
			System.err.println("Properties file not found. Please make sure you have a file called \"AkImporter.properties\" in the same directory as AkImporter.jar");
			System.exit(1); // Stop execution of program
		} catch (IOException e) {
			e.printStackTrace();
		}

		return importerProperties;
	}


	/**
	 * Check if a translation properties file exists that is defined in mab.properties
	 * 
	 * @param	pathToMabPropertiesFile		Path to used mab.properties file
	 * @param	directoryOfTranslationFiles	Directory where translation files are stored
	 * @return	boolean:						true if translation file exists
	 */
	public static boolean translationFilesExist(String pathToMabPropertiesFile, String directoryOfTranslationFiles) {

		// Set some variables:
		Properties mabProperties = new Properties();
		boolean translationFilesExist = false;
		List<String> translationFilenames = new ArrayList<String>();

		// Load .properties file:
		BufferedInputStream propertiesInputStream;
		try {
			propertiesInputStream = new BufferedInputStream(new FileInputStream(pathToMabPropertiesFile));
			mabProperties.load(propertiesInputStream);
			propertiesInputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


		// Create list:
		List<List<String>> lstValues = new ArrayList<List<String>>();

		// Loop through properties:
		for(String key : mabProperties.stringPropertyNames()) {
			// Add properties as list to another list:
			String strValues = mabProperties.getProperty(key);			
			lstValues.add(Arrays.asList(strValues.split("\\s*,\\s*")));
		}

		// Check if there is at least one "translateValue" or "translateValueContains"
		boolean translateValueExists = false;
		for (List<String> lstValue : lstValues) {
			if (lstValue.contains("translateValue") || lstValue.contains("translateValueContains")) {
				translateValueExists = true;
				break;
			}
		}

		if (translateValueExists) {

			// Get all translateValue fields:
			for (List<String> lstValue : lstValues) {
				if (lstValue.toString().contains(".properties")) { // Check if a .properties-File is indicated

					// Get the filename with the help of RegEx:
					//Pattern patternPropFile = java.util.regex.Pattern.compile("[^\\s,;]*\\.properties"); // No (^) whitespaces (\\s), commas or semicolons (,;) before ".properties"-string.
					Pattern patternPropFile = java.util.regex.Pattern.compile("[a-zA-Z0-9]+\\.properties");

					Matcher matcherPropFile = patternPropFile.matcher("");
					for(String lstVal : lstValue) {
						matcherPropFile.reset(lstVal);
						if(matcherPropFile.find()) {
							translationFilenames.add(matcherPropFile.group());
						}
					}
				}
			}

		} else {
			// We don't need a translation file.
			// Return true so that the import script can go on without an error:
			translationFilesExist = true;
		}


		for (String translationFilename : translationFilenames) {
			String pathToTranslationFiles = directoryOfTranslationFiles + File.separator + translationFilename;
			if (fileExists(new File(pathToTranslationFiles)) == false) {
				System.out.println("Die in der .properties-Datei \"" + pathToMabPropertiesFile + "\" angegebene Translation-Datei \"" + translationFilename + "\" existiert im Verzeichnis \"" + directoryOfTranslationFiles + "\" nicht."
						+ "\nBitte stellen Sie sicher, dass die Translation-Datei dort existiert und drücken Sie die Eingabetaste um fortzufahren.");
				translationFilesExist = false;
				return translationFilesExist;
			} else {
				translationFilesExist = true;
			}
		}

		return translationFilesExist;
	}



	private static void postProcess() {
		HttpSolrServer postSolrServerBiblio = new HttpSolrServer(iSolr);

		// Use TreeMap to keep sort order on ppIds:
		Map<Integer, PostProcess> postprocesses = new TreeMap<Integer, PostProcess>();
		SortedSet<Integer> allPpIds = new TreeSet<Integer>();

		// Get all post process IDs that are defined in AkImporter.properties
		for (Entry<Object, Object> entry : importerProperties.entrySet()) {
			String propertiesKey = entry.getKey().toString();
			if (propertiesKey.startsWith("import.postprocess.")) {
				String[] keySegments = propertiesKey.split("\\.");
				int ppId = Integer.valueOf(keySegments[2]);
				allPpIds.add(ppId);
			}
		}

		for (int ppId : allPpIds) {
			String ppAction = (importerProperties.get("import.postprocess."+ppId+".action") != null) ? importerProperties.get("import.postprocess."+ppId+".action").toString() : null;
			String ppQuery = (importerProperties.get("import.postprocess."+ppId+".query") != null) ? importerProperties.get("import.postprocess."+ppId+".query").toString() : null;
			String ppField = (importerProperties.get("import.postprocess."+ppId+".value") != null) ? importerProperties.get("import.postprocess."+ppId+".field").toString() : null;
			String ppValue = (importerProperties.get("import.postprocess."+ppId+".value") != null) ? importerProperties.get("import.postprocess."+ppId+".value").toString() : null;

			if (ppAction != null && ppQuery != null && ppValue != null) {
				PostProcess postprocess = new PostProcess(ppId, ppAction, ppQuery, ppField, ppValue);
				postprocesses.put(ppId, postprocess);
			}
		}

		if (!postprocesses.isEmpty()) {
			new PostProcessor(postSolrServerBiblio, postprocesses, print, optimize);
		}
	}



	/**
	 * Checking properties in AkImporter.properties for bibliographic data updates
	 * 
	 * @param checkFtp	a boolean that indicates wether to check the FTP connection or not
	 * @return			true if everything is OK, false otherwise
	 */
	private static boolean checkUpdateProperties(boolean checkFtp) {

		boolean updatePropertiesOk = false;

		// Check if there are empty values
		for (String key : importerProperties.stringPropertyNames()) {
			if (key.startsWith("update.")  && !key.contains(".customMabProperties") && key.contains("ftp") == checkFtp && key.equals("update.remotePath") == checkFtp) {
				String value = importerProperties.getProperty(key);
				if (value.isEmpty() || value == null) {
					System.err.println("Please specify a valid value for \"" + key + "\" in \"AkImporter.properties\"");
					return updatePropertiesOk;
				}
			}
		}

		// Checking FTP connection and remote path if checkFtp is true:
		boolean ftpIsOk = false;
		String ftpErrorMsg = null;
		if (checkFtp) {
			FTPClient ftpClient = new FTPClient();
			try {
				ftpClient.connect(uFtpHost, uFtpPort);
				boolean isLoggedIn = ftpClient.login(uFtpUser, uFtpPass);
				if (isLoggedIn) {
					boolean isConnected = ftpClient.sendNoOp();
					if (!isConnected) {
						ftpErrorMsg = "FTP connection failed. Check if values \"update.ftpHost\" and \"update.ftpPort\" in \"AkImporter.properties\" are correct.";
					} else { // FTP connection was successful.
						// Check if remote path exists and if files with suffix ".tar.gz" exists:
						FTPFile[] ftpFiles = ftpClient.listFiles(uRemotePath, new FTPFileFilter() {
							@Override
							public boolean accept(FTPFile ftpFile) {
								return (ftpFile.isFile() && ftpFile.getName().endsWith(".tar.gz"));
							}
						});
						if (ftpFiles.length <= 0) {
							ftpErrorMsg = "FTP remote path problem. Check if value \"update.remotePath\" in \"AkImporter.properties\" is correct and if there is at least one file with the suffix \".tar.gz\" in this directory on the FTP server.";
						} else {
							ftpIsOk = true;
						}
						// We can now logout and disconnect.
						ftpClient.logout();
						ftpClient.disconnect();
					}
				} else {
					ftpErrorMsg = "FTP login failed. Check if values \"update.ftpUser\" and \"update.ftpPass\" in \"AkImporter.properties\" are correct.";
				}
			} catch (UnknownHostException e) {
				ftpErrorMsg = "FTP host problem. Check if value \"update.ftpHost\" in \"AkImporter.properties\" is correct.";
			} catch (MalformedServerReplyException e) {
				ftpErrorMsg = "FTP problem. Check if value \"update.ftpPort\" in \"AkImporter.properties\" is correct.";
			} catch (ConnectException e) {
				ftpErrorMsg = "FTP problem. Check if value \"update.ftpPort\" in \"AkImporter.properties\" is correct.";
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}

		} else {
			ftpIsOk = true; // We don't need to check FTP, so this is true.
		}

		if (!ftpIsOk) {
			System.err.println(ftpErrorMsg);
		} else {
			// Check if local path exists (where the downloaded files from the FTP server are saved):
			boolean localPathExists = directoryExists(new File(uLocalPath));
			if (!localPathExists) {
				System.err.println("Directory specified for \"update.localPath\" in \"AkImporter.properties\" does not exist.");
			} else {
				boolean solrRunning = isSolrserverRunning(uSolr);
				if (!solrRunning) {
					System.err.println("Please make sure that your Solr server is running and that you specified a correct value for \"update.solr\" in \"AkImporter.properties\".");
				} else {
					String uDefaultMabPropertiesStr = importerProperties.getProperty("update.defaultMabProperties");
					boolean defaultMabPropertiesIsValid = (uDefaultMabPropertiesStr.equals("D") || uDefaultMabPropertiesStr.equals("C")) ? true : false;
					if (!defaultMabPropertiesIsValid) {
						System.err.println("Please specify a valid value for \"update.defaultMabProperties\" in \"AkImporter.properties\"");
					} else {
						if (uDefaultMabPropertiesStr.equals("C")) {
							boolean customMabPropertiesExists = fileExists(new File(uCustomMabProperties));
							if (!customMabPropertiesExists) {
								System.err.println("Please make sure that the file specified for \"update.customMabProperties\" in \"AkImporter.properties\" exists.");
							} else {
								updatePropertiesOk = true;
							}
						} else {
							updatePropertiesOk = true;
						}
					}
				}
			}
		}

		return updatePropertiesOk;
	}


	/**
	 * Checking properties in AkImporter.properties for authority indexing
	 * 
	 * @return			true if everything is OK, false otherwise
	 */
	private static boolean checkAuthorityProperties() {

		boolean authorityPropertiesOk = false;

		// Check if there are empty values
		for (String key : importerProperties.stringPropertyNames()) {
			if (key.startsWith("authority.") && !key.contains(".customMabProperties") && !key.contains(".integrate.entities")) {
				String value = importerProperties.getProperty(key);
				if (value.isEmpty() || value == null) {
					System.err.println("Please specify a valid value for \"" + key + "\" in \"AkImporter.properties\"");
					return authorityPropertiesOk;
				}
			}
		}

		String[] paths = aPath.split(",");
		boolean xmlFileExists = false;
		for (String path : paths) {
			xmlFileExists = fileExists(new File(path.trim()));
			if (!xmlFileExists) {
				break;
			}
		}

		if (!xmlFileExists) {
			System.err.println("At least one file specified for \"authority.path\" in \"AkImporter.properties\" does not exist.");
		} else {
			boolean solrAuthorityRunning = isSolrserverRunning(aSolrAuth);
			boolean solrBibliographicRunning = isSolrserverRunning(aSolrBibl);
			if (!solrAuthorityRunning || !solrBibliographicRunning) {
				System.err.println("Please make sure that your Solr server is running and that you specified correct values for \"authority.solrAuth\" and \"authority.solrBibl\" in \"AkImporter.properties\".");
			} else {
				String aDefaultMabPropertiesStr = importerProperties.getProperty("authority.defaultMabProperties");
				boolean defaultMabPropertiesIsValid = (aDefaultMabPropertiesStr.equals("D") || aDefaultMabPropertiesStr.equals("C")) ? true : false;
				if (!defaultMabPropertiesIsValid) {
					System.err.println("Please specify a valid value for \"authority.defaultMabProperties\" in \"AkImporter.properties\"");
				} else {
					if (aDefaultMabPropertiesStr.equals("C")) {
						boolean customMabPropertiesExists = fileExists(new File(aCustomMabProperties));
						if (!customMabPropertiesExists) {
							System.err.println("Please make sure that the file specified for \"authority.customMabProperties\" in \"AkImporter.properties\" exists.");
						} else {
							authorityPropertiesOk = true;
						}
					} else {
						authorityPropertiesOk = true;
					}
				}
			}
		}

		return authorityPropertiesOk;

	}

	/**
	 * Checking properties in AkImporter.properties for authority updating via OAI harvesting
	 * 
	 * @return			true if everything is OK, false otherwise
	 */
	private static boolean checkAuthorityUpdateProperties() {

		boolean authorityUpdatePropertiesOk = false;

		// Check if there are empty values
		for (String key : importerProperties.stringPropertyNames()) {
			if (key.startsWith("authority.") && !key.contains(".customMabProperties") && !key.contains("authority.path") && !key.contains(".integrate.entities")) {
				String value = importerProperties.getProperty(key);
				if (value.isEmpty() || value == null) {
					System.err.println("Please specify a valid value for \"" + key + "\" in \"AkImporter.properties\"");
					return authorityUpdatePropertiesOk;
				}
			}
		}

		boolean solrAuthorityRunning = isSolrserverRunning(aSolrAuth);
		boolean solrBibliographicRunning = isSolrserverRunning(aSolrBibl);
		if (!solrAuthorityRunning || !solrBibliographicRunning) {
			System.err.println("Please make sure that your Solr server is running and that you specified correct values for \"authority.solrAuth\" and \"authority.solrBibl\" in \"AkImporter.properties\".");
		} else {

			boolean oaiUrlOk = false;
			try {
				URL url = new URL(aUpdateOaiUrl);
				URLConnection conn = url.openConnection();
				conn.connect();
				oaiUrlOk = true;
			} catch (MalformedURLException e) {
				System.err.println("The URL for \"authority.update.oaiUrl\" in \"AkImporter.properties\" is not valid.");
			} catch (IOException e) {
				System.err.println("The connection to the URL for \"authority.update.oaiUrl\" in \"AkImporter.properties\" cannot be established.");
			}

			if (oaiUrlOk) {
				String aDefaultMabPropertiesStr = importerProperties.getProperty("authority.defaultMabProperties");
				boolean defaultMabPropertiesIsValid = (aDefaultMabPropertiesStr.equals("D") || aDefaultMabPropertiesStr.equals("C")) ? true : false;
				if (!defaultMabPropertiesIsValid) {
					System.err.println("Please specify a valid value for \"authority.defaultMabProperties\" in \"AkImporter.properties\"");
				} else {
					if (aDefaultMabPropertiesStr.equals("C")) {
						boolean customMabPropertiesExists = fileExists(new File(aCustomMabProperties));
						if (!customMabPropertiesExists) {
							System.err.println("Please make sure that the file specified for \"authority.customMabProperties\" in \"AkImporter.properties\" exists.");
						} else {
							authorityUpdatePropertiesOk = true;
						}
					} else {
						authorityUpdatePropertiesOk = true;
					}
				}
			}
		}

		return authorityUpdatePropertiesOk;
	}


	/**
	 * Checking properties in AkImporter.properties for importing bibliographic records
	 * 
	 * @return			true if everything is OK, false otherwise
	 */
	private static boolean checkImportProperties() {

		boolean importPropertiesOk = false;

		// Check if there are empty values
		for (String key : importerProperties.stringPropertyNames()) {
			if (key.startsWith("import.") && !key.contains(".customMabProperties")) {
				String value = importerProperties.getProperty(key);
				if (value.isEmpty() || value == null) {
					System.err.println("Please specify a valid value for \"" + key + "\" in \"AkImporter.properties\"");
					return importPropertiesOk;
				}
			}
		}

		// Check if the values are valid and if the files or directories exist and if the Solr server is running
		boolean datasetIsValid = (iDataset.equals("1") || iDataset.equals("2")) ? true : false;
		if (!datasetIsValid) {
			System.err.println("Please specify a valid value for \"import.dataset\" in \"AkImporter.properties\"");
		} else {
			boolean xmlFileExists = (iDataset.equals("1")) ? fileExists(new File(iPath)) : directoryExists(new File(iPath));
			if (!xmlFileExists) {
				System.err.println("Directory or file specified for \"import.path\" in \"AkImporter.properties\" does not exist.");
			} else {
				String iValidationStr = importerProperties.getProperty("import.validation");
				boolean validationValid = (iValidationStr.equals("V") || iValidationStr.equals("S")) ? true : false;
				if (!validationValid) {
					System.err.println("Please specify a valid value for \"import.validation\" in \"AkImporter.properties\"");
				} else {
					boolean solrRunning = isSolrserverRunning(iSolr);
					if (!solrRunning) {
						System.err.println("Please make sure that your Solr server is running and that you specified a correct value for \"import.solr\" in \"AkImporter.properties\".");
					} else {
						String iDefaultMabPropertiesStr = importerProperties.getProperty("import.defaultMabProperties");
						boolean defaultMabPropertiesIsValid = (iDefaultMabPropertiesStr.equals("D") || iDefaultMabPropertiesStr.equals("C")) ? true : false;
						if (!defaultMabPropertiesIsValid) {
							System.err.println("Please specify a valid value for \"import.defaultMabProperties\" in \"AkImporter.properties\"");
						} else {
							if (iDefaultMabPropertiesStr.equals("C")) {
								boolean customMabPropertiesExists = fileExists(new File(iCustomMabProperties));
								if (!customMabPropertiesExists) {
									System.err.println("Please make sure that the file specified for \"import.customMabProperties\" in \"AkImporter.properties\" exists.");
								} else {
									importPropertiesOk = true;
								}
							} else {
								importPropertiesOk = true;
							}
						}
					}
				}
			}
		}

		return importPropertiesOk;
	}


	/**
	 * Setting up command line options
	 */
	private static void setCLI() {

		// k (AK mode) option - FOR TESTING ONLY
		Option oAkTest = Option
				.builder("k")
				.required(true)
				.longOpt("aktest")
				.desc("ONLY FOR TESTING - DO NOT USE IN PRODUCTION ENVIRONMENT!")
				.build();

		// i (import with interactive mode)
		Option oImport = Option
				.builder("i")
				.required(true)
				.longOpt("interactive")
				.desc("Import metadata from one or multiple MarcXML file(s) using interactive mode")
				.build();

		// p (import with settings in properties file after confirming them)
		Option oProperties = Option
				.builder("p")
				.required(true)
				.longOpt("properties")
				.desc("Import metadata from one or multiple MarcXML file(s) using the properties"
						+ "file after confirming the settings")
				.build();

		// P (import with settings in properties file without confirming them)
		Option oPropertiesSilent = Option
				.builder("P")
				.required(true)
				.longOpt("properties-silent")
				.desc("Import metadata from one or multiple MarcXML file(s) using the properties"
						+ "file without confirming the settings")
				.build();

		// r (reimport)
		Option oReIndex = Option
				.builder("r")
				.required(true)
				.longOpt("reimport")
				.desc("Re-Import all data from MarcXML (initial dataset and all ongoing updates). Uses some "
						+ "of the \"import\" and \"update\" settings from the AkImporter.properties file. They "
						+ "have to be set correctly.")
				.build();

		// R (reindex ongoing data updates)
		Option oReIndexOngoing = Option
				.builder("R")
				.required(true)
				.longOpt("reimport-ongoing")
				.desc("Re-Import all data from ongoing data deliveries only (without initial dataset). "
						+ "Uses some of the \"update\" settings from the AkImporter.properties file. "
						+ "They have to be set correctly.")
				.build();

		// l (link parent and child records)
		Option oLink = Option
				.builder("l")
				.required(true)
				.longOpt("link")
				.desc("Linking existing parent and child records in the main bibliographic Solr index "
						+ "(see setting \"import.solr\" in AkImporter.properties file).")
				.build();

		// l (link parent and child records)
		Option oPostprocess = Option
				.builder()
				.required(false)
				.longOpt("post_process")
				.desc("Execute post processor.")
				.build();

		// u (update from ongoing data delivery)
		Option oUpdate = Option
				.builder("u")
				.required(true)
				.longOpt("update")
				.desc("Update from ongoing data delivery in MarcXML")
				.build();

		// a (index authority data after confirming the settings in the properties file)
		Option oAuthority = Option
				.builder("a")
				.required(true)
				.longOpt("authority")
				.desc("Index authority data after confirming the settings")
				.build();

		// A (authority-silent without confirming the settings in the properties file)
		Option oAuthoritySilent = Option
				.builder("A")
				.required(true)
				.longOpt("authority-silent")
				.desc("Index authority data without confirming the settings")
				.build();

		// e (ernten [harvest] and update authority properties from OAI interface)
		Option oErnten = Option
				.builder("e")
				.required(true)
				.longOpt("ernten")
				.desc("Harvest (german \"ernten\") and update authority records")
				.build();

		// c (consolidate)
		Option oConsolidate = Option
				.builder("c")
				.required(true)
				.longOpt("consolidate")
				.desc("Consolidate all data (initial dataset and all ongoing updates) and get one new "
						+ "file which is up to date")
				.build();

		// h (help)
		Option oHelp = Option
				.builder("h")
				.required(true)
				.longOpt("help")
				.desc("Print help")
				.build();



		// v (verbose)
		Option oVerbose = Option
				.builder("v")
				.required(false)
				.longOpt("verbose")
				.desc("Print detailed process messages")
				.build();

		// o (optimize solr)
		Option oOptimize = Option
				.builder("o")
				.required(false)
				.longOpt("optimize")
				.desc("Optimize Solr")
				.build();

		// t (test)
		Option oTestParameter = Option
				.builder("t")
				.required(false)
				.longOpt("test")
				.desc("Test parameters specified in AkImporter.properties. Can be used with options -p, -P, -r, -R, -u, -a, -A.\nExample: java -jar AkImporter.jar -p -t")
				.build();

		// f (set flag of existance to authority records)
		Option oFlagAuthority = Option
				.builder("f")
				.required(false)
				.longOpt("flag")
				.desc("Set only flag of existance to authority records instead of indexing them. The flag tells us if the authority record is used in at least one bibliographich record. Can be used with -a and -A\nExample: java -jar AkImporter.jar -a -f")
				.build();

		// m (authority-merge)
		Option oAuthorityMerge = Option
				.builder("m")
				.required(false)
				.longOpt("authority-merge")
				.desc("Sets flag of existance to authority and merges authority data into bibliographic data. Can be used with -a, -A, -u.\nExample: java -jar AkImporter.jar -a -m")
				.build();

		// O (OAI-PMH Import/Update)
		Option oOaiImport = Option
				.builder("O")
				.required(false)
				.longOpt("oai_import")
				.hasArg(true)
				.numberOfArgs(1)
				.desc("Indexing or updating data from an OAI-PMH interface.")
				.build();

		// O (OAI-PMH Import/Update)
		Option oOaiReImport = Option
				.builder()
				.required(false)
				.longOpt("oai_reimport")
				.hasArg(true)
				.numberOfArgs(1)
				.desc("Index already downloaded OAI data again.")
				.build();

		// X (XML Import)
		Option oXmlImport = Option
				.builder("X")
				.required(false)
				.longOpt("xml_import")
				.hasArg(true)
				.numberOfArgs(1)
				.desc("Indexing or updating data from an OAI-PMH interface.")
				.build();

		// index_sampledata (index sample data)
		Option oIndexSampleData = Option
				.builder()
				.required(false)
				.longOpt("index_sampledata")
				.desc("Index sample data from AK Bibliothek Wien.")
				.build();
		
		// ak_index (for indexing to the AKindex [a.k.a. browse index] application - has nothing to do with AKsearch/VuFind!)
		Option oAkIndex = Option
				.builder()
				.required(false)
				.longOpt("ak_index")
				.hasArg(true)
				.numberOfArgs(1)
				.desc("Indexing fields for AKindex (a.k.a. browse index)")
				.build();
		
		// ak_index_allfields (generating the "all fields" file for AKindex [a.k.a. browse index] application - has nothing to do with AKsearch/VuFind!)
		Option oAkIndexAllFields = Option
				.builder()
				.required(false)
				.longOpt("ak_index_allfields")
				.desc("Generate the \"all fields\" php file for AKindex (a.k.a. browse index)")
				.build();

		optionGroup.addOption(oAkTest);
		optionGroup.addOption(oImport);
		optionGroup.addOption(oProperties);
		optionGroup.addOption(oPropertiesSilent);
		optionGroup.addOption(oReIndex);
		optionGroup.addOption(oReIndexOngoing);
		optionGroup.addOption(oLink);
		optionGroup.addOption(oPostprocess);
		optionGroup.addOption(oUpdate);
		optionGroup.addOption(oAuthority);
		optionGroup.addOption(oAuthoritySilent);
		optionGroup.addOption(oOaiImport);
		optionGroup.addOption(oOaiReImport);
		optionGroup.addOption(oXmlImport);
		optionGroup.addOption(oConsolidate);
		optionGroup.addOption(oErnten);
		optionGroup.addOption(oHelp);
		optionGroup.addOption(oIndexSampleData);
		optionGroup.addOption(oAkIndex);
		optionGroup.addOption(oAkIndexAllFields);
		optionGroup.setRequired(true);
		options.addOptionGroup(optionGroup);
		options.addOption(oVerbose);
		options.addOption(oOptimize);
		options.addOption(oTestParameter);
		options.addOption(oFlagAuthority);
		options.addOption(oAuthorityMerge);
	}


}