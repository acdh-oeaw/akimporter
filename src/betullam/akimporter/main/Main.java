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

	//static boolean isIndexerTest = false;
	static boolean isUpdate = false;
	static boolean isUpdateSuccessful = false;
	static boolean isIndexingOnly = false;
	static boolean isLinkingOnly = false;
	static boolean isReIndexOngoing = false;
	//static boolean isReIndexAll = false;
	static boolean optimize = true;
	static boolean print = false;
	static boolean isWithCliArgs = false;

	// 1
	static Scanner scanner;
	static String typeOfDataset;
	static String pathToMabXmlFile;
	static String isValidationOk;
	static boolean hasValidationPassed;
	static String isXmlCleanOk;
	static String solrServerAddress;
	static String useDefaultMabPropertiesFile;
	static String pathToMabPropertiesFile;
	static String directoryOfTranslationFiles;
	static boolean areTranslationFilesOk;
	static String propertiesFileInfo;
	static boolean useDefaultMabProperties;
	static String isIndexingOk;
	static boolean isIndexingSuccessful;

	// 2
	static String pathToMultipleXmlFolder;
	static String isMergeOk;
	static String pathToMergedFile;
	static boolean isMergingSuccessful;


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
		 */
		Options options = new Options();
		OptionGroup ogMain = new OptionGroup();

		// i (import) option
		Option oImport = Option
				.builder("i")
				.required(true)
				.longOpt("import")
				.desc("Import metadata from one or multiple MarcXML file(s)")
				.build();

		// u (update) option
		Option oUpdate = Option
				.builder("u")
				.numberOfArgs(7)
				.argName("remotePath localPath host port user password solrAddress defaultSolrMab")
				.required(true)
				.longOpt("update")
				.desc("Update from ongoing data delivery in MarcXML")
				.build();

		// r (reindex) option
		Option oReIndex = Option
				.builder("r")
				.required(true)
				.longOpt("reimport")
				.desc("Re-import all data from MarcXML")
				.build();

		// ro (reindexongoing) option
		Option oReIndexOngoing = Option
				.builder("ro")
				.required(true)
				.longOpt("reimportongoing")
				.desc("Re-import all ongoing data deliveries")
				.build();

		// l (link) option
		Option oLink = Option
				.builder("l")
				.required(true)
				.longOpt("link")
				.desc("Link parents and child volumes")
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


		ogMain.addOption(oImport);
		ogMain.addOption(oUpdate);
		ogMain.addOption(oReIndex);
		ogMain.addOption(oReIndexOngoing);
		ogMain.addOption(oLink);
		ogMain.addOption(oHelp);
		ogMain.setRequired(true);
		options.addOptionGroup(ogMain);
		options.addOption(oVerbose);


		/**
		 * Import with arguments
		 */
		// Single metadata file
		Option oPathToMabXmlFile = Option
				.builder("px")
				.required(false)
				.hasArg()
				.longOpt("pathxml")
				.desc("Full path to single xml file containing metadata, e. g. /home/username/filename.xml")
				.build();

		// Multiple metadata files which has to be merged
		Option oPathToMabXmlFolder = Option
				.builder("pf")
				.required(false)
				.hasArg()
				.longOpt("pathfolder")
				.desc("Full path to folder with multiple xml files containing metadata, e. g. /home/username/xmlfiles")
				.build();

		Option oValidateXml = Option
				.builder("vx")
				.required(false)
				.longOpt("validate")
				.desc("Validate XML file")
				.build();

		Option oSolrUrl = Option
				.builder("su")
				.required(false)
				.hasArg()
				.longOpt("solrurl")
				.desc("Url to the Solr Server including core name, e. g. http://localhost:8080/solr/corename")
				.build();

		Option oOwnMabProps = Option
				.builder("om")
				.required(false)
				.hasArg()
				.longOpt("ownmabprops")
				.desc("Use own .properties file and indicate the full path to it, e. g. /home/username/myownmab.properties")
				.build();

		Option oOptimize = Option
				.builder("os")
				.required(false)
				.longOpt("optimizesolr")
				.desc("Optimize Solr")
				.build();

		// Add options for import from single file
		options.addOption(oPathToMabXmlFile);
		options.addOption(oPathToMabXmlFolder);
		options.addOption(oValidateXml);
		options.addOption(oSolrUrl);
		options.addOption(oOwnMabProps);
		options.addOption(oOptimize);



		try {


			CommandLine cmd = clParser.parse(options, args, true);
			String selectedMainOption = ogMain.getSelected();

			// Verbose?
			if (cmd.hasOption("v")) {
				print = true;
			}


			// Switch between main options
			switch (selectedMainOption) {
			case "i":

				if (cmd.hasOption("px")) { // Import from single file
					new Import(
							"1",
							cmd.getOptionValue("px"),
							null,
							cmd.hasOption("vx"),
							cmd.getOptionValue("su"),
							cmd.hasOption("om"),
							cmd.getOptionValue("om"),
							cmd.hasOption("os"),
							cmd.hasOption("v")
							);
				} else if (cmd.hasOption("pf")) { // Import multiple files from folder

					new Import(
							"2",
							null,
							cmd.getOptionValue("pf"),
							cmd.hasOption("vx"),
							cmd.getOptionValue("su"),
							cmd.hasOption("om"),
							cmd.getOptionValue("om"),
							cmd.hasOption("os"),
							cmd.hasOption("v")
							);
				} else {
					print = true;
					new Import(optimize, print);
				}
				break;


			case "u":
				String[] updateArgs = cmd.getOptionValues("u");				

				String remotePath = updateArgs[0];
				String localPath = updateArgs[1];
				String host = updateArgs[2];
				int port = Integer.valueOf(updateArgs[3]);
				String user = updateArgs[4];
				String password = updateArgs[5];
				String solrAddress = updateArgs[6];

				Updater updater = new Updater();																			
				isUpdateSuccessful = updater.update(remotePath, localPath, host, port, user, password, solrAddress, cmd.hasOption("om"), cmd.getOptionValue("om"), cmd.hasOption("os"), cmd.hasOption("v"));

				break;


			case "r":
				System.out.println("This function \"Reimoprt all\" is not working at the moment. Work in progress ...");
				break;

			case "ro":
				ReImport reImport = new ReImport(cmd.hasOption("v"));
				boolean isReImportingSuccessful = reImport.isReImportingSuccessful();
				if (isReImportingSuccessful) {
					System.out.println("Re-importing successful");
				}
				break;

			case "l":
				// Connect child and parent volumes:
				if (cmd.hasOption("su")) {
					String solrUrl = cmd.getOptionValue("su");
					HttpSolrServer solrServer = new HttpSolrServer(solrUrl);
					Relate relate = new Relate(solrServer, null, false, cmd.hasOption("v"));
					boolean isRelateSuccessful = relate.isRelateSuccessful();
					if (cmd.hasOption("v")) {
						if (isRelateSuccessful) {
							System.out.println("Done linking parent and child records. Everything was successful.");
						}
					}
				} else {
					System.out.println("Missing option -su (--solrurl). Please specify also a Solr Server URL incl. core name, e. g. -su http://localhost:8080/solr/corename");
				}
				break;


			case "h":
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.printHelp("AkImporter", "", options, "", true);


			default:
				break;
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
