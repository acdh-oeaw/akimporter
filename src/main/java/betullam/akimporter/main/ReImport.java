/**
 * Re-import already imported records. This could be usefull to apply new functions while importing records.
 * Example: A new MAB field should be indexed, that's why we have to re-import the already indexed records
 * 			again, so that the field in question will be available to Solr for them too, and not only for
 * 			records imported in the future.
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
package main.java.betullam.akimporter.main;

import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.solrmab.Index;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class ReImport {

	private Scanner scanner = null;
	private HttpSolrServer solrServer = null;
	private String timeStamp = null;
	private boolean print = false;
	private boolean optimize = false;
	private SolrMabHelper smHelper = new SolrMabHelper();
	boolean isReImportingSuccessful = false;

	/**
	 * Constructor for starting a re-import of already imported data.
	 *  
	 * @param	print	boolean: Print status messages to console?
	 */
	public ReImport(boolean print, boolean optimize) {
		this.print = print;
		this.optimize = optimize;
		this.scanner = new Scanner(System.in);
	}


	/**
	 * Starts the re-import process (none-interactive) for ongoing data deliveries:
	 * 
	 * @param pathToUpdateDir			String: path to the local directory where the files from the update-process are saved
	 * @param isValidationOk			boolean: Indicates if the data should be validated
	 * @param solrServerAddress			String: Solr Server incl. core name to which the data should be indexed.
	 * @param useDefaultMabProperties	boolean: Indicates if the default mab.properties should be used
	 * @param pathToMabPropertiesFile	String: Path to custom .properties if the default mab.properties file is not used
	 */
	public void reImportOngoing(String pathToUpdateDir, boolean isValidationOk, String solrServerAddress, boolean useDefaultMabProperties, String pathToMabPropertiesFile) {

		// Get a sorted list (oldest to newest) from all ongoing data deliveries:
		File fPathToMergedDir = new File(stripFileSeperatorFromPath(pathToUpdateDir) + File.separator + "merged");
		List<File> fileList = (List<File>)FileUtils.listFiles(fPathToMergedDir, new String[] {"xml"}, true); // Get all xml-files recursively
		Collections.sort(fileList); // Sort oldest to newest

		boolean allFilesValid = false;

		if (isValidationOk) {
			this.smHelper.print(this.print, "\nStart validating all data ...");
			XmlValidator bxh = new XmlValidator();

			for (File file : fileList) {
				boolean hasValidationPassed = bxh.validateXML(file.getAbsolutePath());

				if (hasValidationPassed) {
					allFilesValid = true;
				} else {
					allFilesValid = false;
					System.err.println("Error in file " + file.getName() + ". Import process was cancelled.");
					return;
				}
			}

			// If all files are valid, go on with the import process
			if (allFilesValid) {
				this.smHelper.print(this.print, "\nValidation of all data was successful.\n");

				// If there are errors in at lease one file, stop the import process:
			} else {
				System.err.println("\nError while validating. Import process was cancelled!\n");
				return;
			}
		} else {
			this.smHelper.print(this.print, "\nSkipped validation!");
		}

		// Variables
		String propertiesFileInfo = null;
		//boolean useDefaultMabProperties = true;
		String directoryOfTranslationFiles = null;

		if (useDefaultMabProperties) {
			//pathToMabPropertiesFile = "/betullam/akimporter/resources/mab.properties";
			//directoryOfTranslationFiles = "/betullam/akimporter/resources";
			pathToMabPropertiesFile = "/mab.properties";
			directoryOfTranslationFiles = "/";
			propertiesFileInfo = "Using default mab.properties file";
		} else {
			propertiesFileInfo = "Using custom .properties file: " + pathToMabPropertiesFile;
			directoryOfTranslationFiles = new File(pathToMabPropertiesFile).getParent();
			boolean areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile, directoryOfTranslationFiles);

			// It the translation files, that are defined in the custom MAB properties file, do not exist
			// (they have to be in the same directory), that give an appropriate message:
			if (areTranslationFilesOk == false) {
				System.err.println("\nCould not find translation file! Please look into your custom .properties file"
						+ "for the MAB data an check if the translations-files you specified there exist in the same"
						+ "directory.\n");
				return;
			}
		}

		this.smHelper.print(this.print, propertiesFileInfo + "\n");

		boolean isIndexingSuccessful = false;
		boolean isRelateSuccessful = false;

		// Create SolrSever:
		this.solrServer = new HttpSolrServer(solrServerAddress);


		for (File file : fileList) {
			this.timeStamp = String.valueOf(new Date().getTime());

			this.smHelper.print(this.print, "\nIndexing file " + (fileList.indexOf(file)+1) + " of " + fileList.size() + "\n");
			String originalTimestamp = file.getName().replace(".xml", "");
			String originalUpDate = DateFormatUtils.format(Long.valueOf(originalTimestamp), "dd.MM.yyyy HH:mm:ss");
			this.smHelper.print(this.print, "Original update time: " + originalUpDate + "\n");
			
			// Index metadata to Solr
			Index index = new Index(file.getAbsolutePath(), this.solrServer, useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, false, this.print);
			isIndexingSuccessful = index.isIndexingSuccessful();

			// Connect child and parent volumes:
			Relate relate = new Relate(this.solrServer, this.timeStamp, false, this.print);
			isRelateSuccessful = relate.isRelateSuccessful();

			// If a file could not be indexed, stop import process:
			if (!isIndexingSuccessful && !isRelateSuccessful) {
				System.err.println("\nError while indexing!\n");
				return;
			}
		}

		if (isIndexingSuccessful && isRelateSuccessful) {
			if(this.optimize) {
				this.smHelper = new SolrMabHelper(this.solrServer);
				this.smHelper.print(print, "\nStart optimizing Solr index. This could take a while. Please wait ...");
				this.smHelper.solrOptimize();
			}
			this.smHelper.print(this.print, "\nRe-Importing of ongoing data updates is sucessfully done.\n");
			this.isReImportingSuccessful = true;
		} else {
			System.err.println("\nError while re-importing ongoing data updates!\n");
			return;
		}


		return;
	}


	/**
	 * Starts interactive re-import of ongoing data deliveries:
	 * 
	 * @param	none
	 * @return	void
	 */
	public void reImportInteractive() {
		// Ask user for path to "merged" directory:
		String pathToMergedDir = Main.getUserInput("\nSpecify the path to the \"merged\" folder?\n Example: /home/username/datenlieferungen/merged)?", "directoryExists", scanner);

		// Get a sorted list (oldest to newest) from all ongoing data deliveries:
		File fPathToMergedDir = new File(pathToMergedDir);
		List<File> fileList = (List<File>)FileUtils.listFiles(fPathToMergedDir, new String[] {"xml"}, true); // Get all xml-files recursively
		Collections.sort(fileList); // Sort oldest to newest

		// Ask user if he wants to start or skip the validation of the files or if he wants to stop the import process:
		String isValidationOk = Main.getUserInput("\nThe xml file must be validated. This can take a while. The original data won't be changed."
				+ " To continue, you may validate or skip the validation. Be aware that skipping the validation may cause problems if there"
				+ " are errors in the xml file. If you cancel, the whole import process will be cancelled! "
				+ "\n V = Validate \n S = Skip \n C = Cancel", "V, S, C", scanner);

		// Start or skip validation of the files:
		if (isValidationOk.equals("V") || isValidationOk.equals("S")) {

			boolean allFilesValid = false;

			if (isValidationOk.equals("V")) {
				this.smHelper.print(this.print, "\nStart validating data ...");
				XmlValidator bxh = new XmlValidator();

				for (File file : fileList) {
					boolean hasValidationPassed = bxh.validateXML(file.getAbsolutePath());

					if (hasValidationPassed) {
						allFilesValid = true;
					} else {
						allFilesValid = false;
						System.err.println("Error in file " + file.getName() + ". Import process was cancelled.");
						return;
					}
				}

				// If all files are valid, go on with the import process
				if (allFilesValid) {
					this.smHelper.print(this.print, "\nValidation was successful.\n");

					// If there are errors in at lease one file, stop the import process:
				} else {
					System.err.println("\nError while validating. Import process was cancelled!\n");
					return;
				}
			} else if (isValidationOk.equals("S")) {
				this.smHelper.print(this.print, "\nSkipped validation!");
			}

			// At this point, all files should have passed the validation process. No, ask the user for the Solr server address:
			String solrServerAddress = Main.getUserInput("\nSpecify the Solr Server address (URL) incl. core name (e. g. http://localhost:8080/solr/corename)", "solrPing", scanner);

			// Ask user if he want's to use the default mab.properties or his own mab.properties:
			String useDefaultMabPropertiesFile = Main.getUserInput("\nDo you want to use the default \"mab.properties\" file? "
					+ "If not, you can specify your own custom .properties file."
					+ "\n D = Yes, default\n N = No, custom file", "D, N", scanner);

			// Variables
			String propertiesFileInfo = null;
			boolean useDefaultMabProperties = true;
			String pathToMabPropertiesFile = null;
			String directoryOfTranslationFiles = null;

			if (useDefaultMabPropertiesFile.equals("D")) {
				useDefaultMabProperties = true;
				//pathToMabPropertiesFile = "/betullam/akimporter/resources/mab.properties";
				//directoryOfTranslationFiles = "/betullam/akimporter/resources";
				pathToMabPropertiesFile = "/mab.properties";
				directoryOfTranslationFiles = "/";
				propertiesFileInfo = "Use default mab.properties file";
			} else {
				useDefaultMabProperties = false;
				pathToMabPropertiesFile = Main.getUserInput("\nSpecify a path to your own custom .properties file (e. g. /home/username/my.properties)."
						+ " Please be aware that the file suffix must be \".properties\".", "propertiesExists", scanner);
				propertiesFileInfo = "Use custom .properties file: " + pathToMabPropertiesFile;
				directoryOfTranslationFiles = new File(pathToMabPropertiesFile).getParent();
				boolean areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile, directoryOfTranslationFiles);

				// It the translation files, that are defined in the custom MAB properties file, do not exist
				// (they have to be in the same directory), that give an appropriate message:
				while (areTranslationFilesOk == false) {
					scanner.nextLine();
					areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile ,directoryOfTranslationFiles);
				}
			}

			String isIndexingOk = Main.getUserInput("\nEverything is ready now. Please review your choices:"
					+ "\n Merged folder:\t" + pathToMergedDir
					+ "\n Solr Server:\t" + solrServerAddress
					+ "\n .properties:\t" + propertiesFileInfo
					+ "\n\nDo you want to begin the import process?"
					+ "\nATTENTION: Depending on the amount of data and the performance of the computer, the import process could take quite some time."
					+ " So time to grab a coffee or a beer :-) "
					+ " \n Y = Yes, start import process\n N = No, cancel import process", "Y, N", scanner);

			if (isIndexingOk.equals("Y")) {

				boolean isIndexingSuccessful = false;
				boolean isRelateSuccessful = false;

				// Create SolrSever:
				this.solrServer = new HttpSolrServer(solrServerAddress);


				for (File file : fileList) {
					this.timeStamp = String.valueOf(new Date().getTime());

					this.smHelper.print(this.print, "\nIndexing file " + (fileList.indexOf(file)+1) + " of " + fileList.size() + "\n");
					String originalTimestamp = file.getName().replace(".xml", "");
					String originalUpDate = DateFormatUtils.format(Long.valueOf(originalTimestamp), "dd.MM.yyyy HH:mm:ss");
					this.smHelper.print(this.print, "Original update time: " + originalUpDate + "\n");
					
					// Index metadata so Solr
					Index index = new Index(file.getAbsolutePath(), this.solrServer, useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, false, this.print);
					isIndexingSuccessful = index.isIndexingSuccessful();

					// Connect child and parent volumes:
					Relate relate = new Relate(this.solrServer, this.timeStamp, false, this.print);
					isRelateSuccessful = relate.isRelateSuccessful();

					// If a file could not be indexed, stop import process:
					if (!isIndexingSuccessful && !isRelateSuccessful) {
						System.err.println("\nError while indexing!\n");
						return;
					}
				}

				if (isIndexingSuccessful && isRelateSuccessful) {
					if(this.optimize) {
						this.smHelper = new SolrMabHelper(this.solrServer);
						this.smHelper.print(print, "\nStart optimizing Solr index. This could take a while. Please wait ...");
						this.smHelper.solrOptimize();
					}
					this.smHelper.print(this.print, "\nImport process was successful.\n");
					this.isReImportingSuccessful = true;
				} else {
					System.err.println("\nError while importing!\n");
					return;
				}
			} else {
				this.smHelper.print(this.print, "\nImport process cancelled as requested by user.\n");
				return;
			}


		} else { // Stop inport process
			System.out.println("\nImport process cancelled as requested by user.");
			return;
		}

		return;
	}



	/**
	 * Check if the re-import process was successful.
	 * 
	 * @return	boolean: true if the process was successful
	 */
	public boolean isReImportingSuccessful() {
		return this.isReImportingSuccessful;
	}

	/**
	 * Remove last file separator character of a String representing a path to a directory
	 *  
	 * @param path	A string representing a path to a directory.
	 * @return		The path without the last file separator character.
	 */
	private static String stripFileSeperatorFromPath(String path) {
		if (!path.equals(File.separator) && (path.length() > 0) && (path.charAt(path.length()-1) == File.separatorChar)) {
			path = path.substring(0, path.length()-1);
		}
		return path;
	}



}
