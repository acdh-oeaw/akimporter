/**
 * This is the main file of AkImporter that is started when the programm is invoked.
 *
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
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
package betullam.akimporter.main;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;

import betullam.akimporter.solrmab.Relate;
import betullam.akimporter.updater.Updater;

/** TODO:
 * 
 * 1. Tidy up code
 * 2. Better command line options
 * 3. Better error messages in try-catch blocks.
 * 4. Better logging.
 * 5. Translation console output to german?
 *
 */

public class Main {
	

	// General
	static boolean optimize = false;
	static boolean print = false;
	static boolean isUpdateSuccessful = false;

	// Get settings from AkImporter.properties file and set them to variables
	static Properties importerProperties = getImporterProperties("AkImporter.properties");
	static String iDataset = importerProperties.getProperty("import.dataset");
	static String iPath = importerProperties.getProperty("import.path");
	static boolean iValidation = (importerProperties.getProperty("import.validation").equals("V")) ? true : false;
	static String iSolr = importerProperties.getProperty("import.solr");
	static boolean iDefaultMabProperties = (importerProperties.getProperty("import.defaultMabProperties").equals("D")) ? true : false;
	static String iCustomMabProperties = importerProperties.getProperty("import.customMabProperties");
	static String uFtpHost = importerProperties.getProperty("update.ftpHost");
	static int uFtpPort = Integer.valueOf(importerProperties.getProperty("update.ftpPort"));
	static String uFtpUser = importerProperties.getProperty("update.ftpUser");
	static String uFtpPass = importerProperties.getProperty("update.ftpPass");
	static String uRemotePath = importerProperties.getProperty("update.remotePath");
	static String uLocalPath = importerProperties.getProperty("update.localPath");
	static String uSolr = importerProperties.getProperty("update.solr");
	static boolean uDefaultMabProperties = (importerProperties.getProperty("update.defaultMabProperties").equals("D")) ? true : false;
	static String uCustomMabProperties = importerProperties.getProperty("update.customMabProperties");
	static String aPath = importerProperties.getProperty("authority.path");
	static String aSolrAuth = importerProperties.getProperty("authority.solrAuth");
	static String aSolrBibl = importerProperties.getProperty("authority.solrBibl");
	static boolean aDefaultMabProperties = (importerProperties.getProperty("authority.defaultMabProperties").equals("D")) ? true : false;
	static String aCustomMabProperties = importerProperties.getProperty("authority.customMabProperties");
	
	


	/**
	 * Main method of AkImporter.
	 * This is the starting point for all following actions.
	 * 
	 * @param	args	Command line arguments
	 */
	public static void main(String[] args) {
        
		// Log4J
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.WARN);
		CommandLineParser clParser = new DefaultParser();



		/**
		 * Main options
		 * 
		 * TODO: Realize these options with the new AkImporter.properties file (see above):
		 * 
		 * NOTES:
		 *  To "import" means indexing data and linking of parent and child records.
		 *  To "index" means indexing data without linking parent and child records.
		 *  To "link" means linking only existing parent and child records in the index.
		 * 
		 * -i:
		 * Normal interactive import (user answers questions)
		 * 
		 * -p:
		 * Automatic import with use of AkImporter.properties file.
		 * Show properties and let the user confirm them before
		 * starting import process.
		 * 
		 * -P:
		 * Automatic import with use of AkImporter.properties file.
		 * Do not show properties and do not let the user confirm them.
		 * Import should starts immediately. Mainly for fast testing.
		 * 
		 * -r:
		 * Re-Import all data from MarcXML (initial dataset and all ongoing updates).
		 * Uses settings from the AkImporter.properties file.
		 * 
		 * -R:
		 * Re-Import all data from ongoing data deliveries only (without initial dataset).
		 * Uses settings from the AkImporter.properties file.
		 * 
		 * -l:
		 * Linking existing parent and child records in the main bibliographic Solr index
		 * (see setting "import.solr" in AkImporter.properties file).
		 * 
		 * -u
		 * Update mode. Use this for automatically importing ongoing data
		 * deliveries using a cron job.
		 * 
		 * -a:
		 * Authority mode. Import authority records.
		 * Show properties and let the user confirm them before
		 * starting import process.
		 * 
		 * -A:
		 * Authority mode. Import authority records.
		 * Do not show properties and do not let the user confirm them.
		 * Import should starts immediately. Mainly for fast testing.
		 * 
		 * -h
		 * Show help
		 * 
		 * 
		 * 
		 * OPTIONAL PARAMETERS
		 * -------------------
		 * 
		 * -v
		 * Verbose: Print status messages.
		 * 
		 * -o
		 * Optimize solr index.
		 * 
		 */


		Options options = new Options();
		OptionGroup ogMain = new OptionGroup();

		// i (interactive) option
		Option oImport = Option
				.builder("i")
				.required(true)
				.longOpt("interactive")
				.desc("Import metadata from one or multiple MarcXML file(s) using interactive mode")
				.build();

		// p (import) option
		Option oProperties = Option
				.builder("p")
				.required(true)
				.longOpt("properties")
				.desc("Import metadata from one or multiple MarcXML file(s) using the properties"
						+ "file after confirming the settings")
				.build();

		// P (import) option
		Option oPropertiesSilent = Option
				.builder("P")
				.required(true)
				.longOpt("properties-silent")
				.desc("Import metadata from one or multiple MarcXML file(s) using the properties"
						+ "file without confirming the settings")
				.build();

		// r (reimport) option
		Option oReIndex = Option
				.builder("r")
				.required(true)
				.longOpt("reimport")
				.desc("Re-Import all data from MarcXML (initial dataset and all ongoing updates). Uses some "
						+ "of the \"import\" and \"update\" settings from the AkImporter.properties file. They "
						+ "have to be set correctly.")
				.build();

		// R (reindex ongoing) option
		Option oReIndexOngoing = Option
				.builder("R")
				.required(true)
				.longOpt("reimport-ongoing")
				.desc("Re-Import all data from ongoing data deliveries only (without initial dataset). "
						+ "Uses some of the \"update\" settings from the AkImporter.properties file. "
						+ "They have to be set correctly.")
				.build();

		// l (link) option
		Option oLink = Option
				.builder("l")
				.required(true)
				.longOpt("link")
				.desc("Linking existing parent and child records in the main bibliographic Solr index (see setting \"import.solr\" in AkImporter.properties file).")
				.build();

		// u (update) option
		Option oUpdate = Option
				.builder("u")
				//.numberOfArgs(7)
				//.argName("remotePath localPath host port user password solrAddress defaultSolrMab")
				.required(true)
				.longOpt("update")
				.desc("Update from ongoing data delivery in MarcXML")
				.build();

		// a (authority) option
		Option oAuthority = Option
				.builder("a")
				.required(true)
				.longOpt("authority")
				.desc("Index authority data after confirming the settings")
				.build();

		// A (authority-silent) option
		Option oAuthoritySilent = Option
				.builder("A")
				.required(true)
				.longOpt("authority-silent")
				.desc("Index authority data without confirming the settings")
				.build();

		// h (help) option
		Option oHelp = Option
				.builder("h")
				.required(true)
				.longOpt("help")
				.desc("Print help")
				.build();

		// v (verbose) option
		Option oVerbose = Option
				.builder("v")
				.required(false)
				.longOpt("verbose")
				.desc("Print detailed process messages")
				.build();

		// o (optimize) option
		Option oOptimize = Option
				.builder("o")
				.required(false)
				.longOpt("optimize")
				.desc("Optimize Solr")
				.build();


		ogMain.addOption(oImport);
		ogMain.addOption(oProperties);
		ogMain.addOption(oPropertiesSilent);
		ogMain.addOption(oReIndex);
		ogMain.addOption(oReIndexOngoing);
		ogMain.addOption(oLink);
		ogMain.addOption(oUpdate);
		ogMain.addOption(oAuthority);
		ogMain.addOption(oAuthoritySilent);
		ogMain.addOption(oHelp);
		ogMain.setRequired(true);
		options.addOptionGroup(ogMain);
		options.addOption(oVerbose);
		options.addOption(oOptimize);


		try {

			CommandLine cmd = clParser.parse(options, args, true);
			String selectedMainOption = ogMain.getSelected();

			// Verbose?
			if (cmd.hasOption("v")) {
				print = true;
			}

			// Optimize?
			if (cmd.hasOption("o")) {
				optimize = true;
			}


			// Switch between main options
			switch (selectedMainOption) {
			case "i": {
				new Import(optimize, print);
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
					// Start import process:
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
				} else if (startImport.equals("N")){
					System.out.println("\nImport process cancelled as requested by user.\n");
				}

				break;
			}

			case "P": {
				// Start import process with settings in AkImporter.properties immediately:
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
				break;
			}

			case "r": {
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
				} else {
					System.err.println("Error while re-importing of ongoing data updates.");
				}
				break;
			}

			case "R": {
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
				break;
			}

			case "l": {
				// Connect child and parent volumes:	
				HttpSolrServer solrServer = new HttpSolrServer(iSolr);
				Relate relate = new Relate(solrServer, null, optimize, print);
				boolean isRelateSuccessful = relate.isRelateSuccessful();
				if (cmd.hasOption("v")) {
					if (isRelateSuccessful) {
						System.out.println("Done linking parent and child records. Everything was successful.");
					}
				}
				
				break;
			}

			case "u": {
				Updater updater = new Updater();																			
				isUpdateSuccessful = updater.update (
						uRemotePath,
						uLocalPath,
						uFtpHost,
						uFtpPort,
						uFtpUser,
						uFtpPass,
						uSolr,
						uDefaultMabProperties,
						uCustomMabProperties,
						optimize,
						print
						);
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
					// Start import process for authority records:
					Authority auth = new Authority(
							aPath,
							aDefaultMabProperties,
							aCustomMabProperties,
							aSolrAuth,
							null,
							print,
							optimize
					);
					auth.indexAuthority();
				} else if (startImport.equals("N")){
					System.out.println("\nImport process cancelled as requested by user.\n");
				}
				
				break;
			}

			case "A": {
				// Start import process for authority records:
				Authority auth = new Authority(
						aPath,
						aDefaultMabProperties,
						aCustomMabProperties,
						aSolrAuth,
						null,
						print,
						optimize
				);
				auth.indexAuthority();
				
				break;
			}

			case "h": {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.printHelp("AkImporter", "", options, "", true);
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
	 * @return	String:				The value the user specified
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

	public static Properties getImporterProperties(String pathToImporterProperties) {
		Properties importerProperties = new Properties();
		//Map<String, String> importerProperties = new HashMap<String, String>();

		// Load .properties file:
		BufferedInputStream propertiesInputStream;
		try {
			propertiesInputStream = new BufferedInputStream(new FileInputStream(pathToImporterProperties));
			importerProperties.load(propertiesInputStream);
			propertiesInputStream.close();
		} catch (FileNotFoundException e) {
			System.out.println("Properties file not found. Please make sure you have a file called \"AkImporter.properties\" in the same directory as AkImporter.jar");
			System.out.println("\nStack trace:\n");
			e.printStackTrace();
			return null;
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


		/*
		// Loop through properties:
		for(String key : mabProperties.stringPropertyNames()) {

			// Create list:
			String strValues = mabProperties.getProperty(key);
			List<String> lstValues = new ArrayList<String>();
			lstValues.addAll(Arrays.asList(strValues.split("\\s*,\\s*")));

			// Get all translateValue fields:
			if (lstValues.contains("translateValue")) {
				if (lstValues.toString().contains(".properties")) { // Check if a .properties-File is indicated

					// Get the filename with the help of RegEx:
					Pattern patternPropFile = java.util.regex.Pattern.compile("[^\\s,;]*\\.properties"); // No (^) whitespaces (\\s), commas or semicolons (,;) before ".properties"-string.
					Matcher matcherPropFile = patternPropFile.matcher("");
					for(String lstValue : lstValues) {
						matcherPropFile.reset(lstValue);
						if(matcherPropFile.find()) {
							translationFilenames.add(matcherPropFile.group());
						}
					}
				}
			}
		}
		 */

		// Create list:
		List<List<String>> lstValues = new ArrayList<List<String>>();

		// Loop through properties:
		for(String key : mabProperties.stringPropertyNames()) {
			// Add properties as list to another list:
			String strValues = mabProperties.getProperty(key);			
			lstValues.add(Arrays.asList(strValues.split("\\s*,\\s*")));
		}

		// Check if there is at least one "translateValue"
		boolean translateValueExists = false;
		for (List<String> lstValue : lstValues) {
			if (lstValue.contains("translateValue")) {
				translateValueExists = true;
				break;
			}
		}

		if (translateValueExists) {

			// Get all translateValue fields:
			for (List<String> lstValue : lstValues) {
				if (lstValue.toString().contains(".properties")) { // Check if a .properties-File is indicated

					// Get the filename with the help of RegEx:
					Pattern patternPropFile = java.util.regex.Pattern.compile("[^\\s,;]*\\.properties"); // No (^) whitespaces (\\s), commas or semicolons (,;) before ".properties"-string.
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


}
