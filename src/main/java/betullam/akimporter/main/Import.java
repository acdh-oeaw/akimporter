/**
 * Import to Solr from MarcXML with MAB fields.
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

import java.io.File;
import java.util.Date;
import java.util.Scanner;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.xmlhelper.XmlCleaner;
import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.solrmab.Index;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class Import {

	private HttpSolrServer solrServer = null;
	private Scanner scanner = null;
	private String timeStamp = null;
	private boolean optimize = false;
	private boolean print = true;
	private String typeOfDataset = null;
	private String pathToMabXmlFile = null;
	private String pathToMultipleXmlFolder = null;
	private String isMergeOk = null;
	private String pathToMergedFile = null;
	private String isValidationOk = null;
	private String isXmlCleanOk = null;
	private String solrServerAddress = null;
	private String useDefaultMabPropertiesFile = null;
	private String pathToMabPropertiesFile = null;
	private String isIndexingOk = null;
	private String directoryOfTranslationFiles = null;
	private String propertiesFileInfo = null;
	private long startTime;
	private long endTime;
	private SolrMabHelper smHelper = new SolrMabHelper();

	private boolean isMergingSuccessful = false;
	private boolean hasValidationPassed = false;
	private boolean useDefaultMabProperties = true;
	private boolean areTranslationFilesOk = false;
	private boolean isIndexingSuccessful = false;
	private boolean isRelateSuccessful = false;
	private boolean isWithCliArgs = false;
	private boolean indexSampleData = false;


	/**
	 * Constructor for starting the import process with interactive user inputs.
	 * 
	 * @param	optimize	boolean: Optimize Solr core after indexing? Is recommended, but could take quite some time. You could say false for quick testing. 
	 * @param	print		boolean: Print status messages to console?
	 */
	public Import(boolean optimize, boolean print) {	
		this.optimize = optimize;
		this.print = print;
		this.scanner = new Scanner(System.in);
		this.startImporting();
	}

	/**
	 * Constructor for starting the importing process with command line parameters instead of an interactive user inputs.
	 * 
	 * @param	typeOfDataset				String. Type of dataset: 1 for one big XML file, 2 for many small files
	 * @param	pathToMabXmlFile			String or null. If type of dataset is 1: full path to the XML file. If type of dataset is 2: null
	 * @param	pathToMultipleXmlFolder		String or null.If type of dataset is 2: full path to the directory with XML files. If type of dataset is 1: null
	 * @param	validate					boolean: Should the XML file(s) be validated (searching for errors in XML)?
	 * @param	solrUrl						String: URL to Solr incl. core name where the data should be indexed, e. g.: http://my.solr:8080/corename
	 * @param	ownMabProps					boolean: Use the default or your own custom mab.properties file? true for default.
	 * @param	pathToOwnMabProps			String: If using custom mab.properties file: full path to that file. If using default file: null
	 * @param	optimize					boolean: Optimize Solr core after indexing? Is recommended, but could take quite some time. You could say false for quick testing. 
	 * @param	print						boolean: Print status messages to console?
	 */
	public Import(String typeOfDataset, String pathToMabXmlFile, String pathToMultipleXmlFolder, boolean validate, String solrUrl, boolean defaultMabProps, String pathToCustomMabProps, boolean optimize, boolean print) {
		isMergeOk = "Y";
		isXmlCleanOk = "Y";
		isIndexingOk = "Y";

		this.isWithCliArgs = true;
		this.typeOfDataset = typeOfDataset;
		this.pathToMabXmlFile = pathToMabXmlFile;
		this.pathToMultipleXmlFolder = pathToMultipleXmlFolder;
		this.isValidationOk = (validate) ? "V" : "S";
		this.solrServerAddress = solrUrl;	
		this.useDefaultMabPropertiesFile = (defaultMabProps) ? "D" : "C";
		this.pathToMabPropertiesFile = (defaultMabProps) ? null : pathToCustomMabProps;
		this.optimize = optimize;
		this.print = print;
		this.startImporting();
	}
	
	// Constructor for indexing sample data:
	public Import(boolean indexSampleData, String solrUrl, boolean defaultMabProps, String pathToCustomMabProps) {
		typeOfDataset = "1";
		isMergeOk = "Y";
		isXmlCleanOk = "Y";
		isIndexingOk = "Y";
		
		this.indexSampleData = true;
		this.isWithCliArgs = true;
		this.typeOfDataset = "1";
		this.pathToMabXmlFile = null;
		this.pathToMultipleXmlFolder = null;
		this.isValidationOk = "S";
		this.solrServerAddress = solrUrl;	
		this.useDefaultMabPropertiesFile = (defaultMabProps) ? "D" : "C";
		this.pathToMabPropertiesFile = (defaultMabProps) ? null : pathToCustomMabProps;
		this.optimize = true;
		this.print = true;
		this.startImporting();
	}
	

	/**
	 * Actually starts the importing process.
	 * 
	 * @return	void
	 */
	private void startImporting() {

		if (this.timeStamp == null) {
			this.timeStamp = String.valueOf(new Date().getTime());
		}

		if (!isWithCliArgs) {
			typeOfDataset = Main.getUserInput("\nKind of dataset?\n 1 = one big xml file\n 2 = multiple xml files)?", "1, 2", scanner);
		}

		if (typeOfDataset.equals("1")) { // We have one big XML file
			if (!isWithCliArgs) {
				pathToMabXmlFile = Main.getUserInput("\nWhat is the path to the xml file?\n Example: /home/username/filename.xml)?", "fileExists", scanner);
			}

		} else if (typeOfDataset.equals("2")) { // We have multiple smaller XML files - we need to merge them!
			if (!isWithCliArgs) {
				pathToMultipleXmlFolder = Main.getUserInput("\nWhat is the path to the folder with the xml files?\n Example: /home/username/folder/of/xmlfiles)?", "directoryExists", scanner);
			}
			
			String tempDir = System.getProperty("java.io.tmpdir");
			tempDir = (tempDir.endsWith("/") || tempDir.endsWith("\\")) ? tempDir : tempDir + System.getProperty("file.separator");
			pathToMergedFile = tempDir + "ImporterMergedFile.xml";

			if (!isWithCliArgs) {
				isMergeOk = Main.getUserInput("\nThe xml files will be merged into one xml file now."
						+ " It will be saved in the temporary folder of the system under " + pathToMergedFile + ". The original data"
						+ " won't be changed. Do you want to continue? If not, the whole import process will be cancelled!"
						+ "\n Y = Yes, merge\n N = No, cancel", "Y, N", scanner);
			}

			if (isMergeOk.equals("Y")) {
				XmlMerger xmlm = new XmlMerger(); // Start merging
				isMergingSuccessful = xmlm.mergeElements(pathToMultipleXmlFolder, pathToMergedFile, "collection", "record", 1);

				if (isMergingSuccessful) {
					pathToMabXmlFile = pathToMergedFile;
					this.smHelper.print(this.print, "\nMerging into file " + pathToMergedFile + " was successful.\n");
				} else {
					System.err.println("\nError while merging! Cancelled import process.\n");
					return;
				}
			} else {
				this.smHelper.print(this.print, "\nImport process cancelled as requested by user.\n");
				return;
			}
		}


		if (!isWithCliArgs) {
			isValidationOk = Main.getUserInput("\nThe xml file must be validated. This can take a while. The original data won't be changed."
					+ " To continue, you may validate or skip the validation. Be aware that skipping the validation may cause problems if there"
					+ " are errors in the xml file. If you cancel, the whole import process will be cancelled! "
					+ "\n V = Validate \n S = Skip \n C = Cancel", "V, S, C", scanner);
		}

		if (isValidationOk.equals("V") || isValidationOk.equals("S")) {

			if (isValidationOk.equals("V")) {
				this.smHelper.print(this.print, "\nStarted validation, please be patient ...");
				XmlValidator bxh = new XmlValidator();
				hasValidationPassed = bxh.validateXML(pathToMabXmlFile);

				while (hasValidationPassed == false) {
					this.smHelper.print(this.print, "\nFound a problem in xml file!");
					if (!isWithCliArgs) {
						isXmlCleanOk = Main.getUserInput("\nDo you want to continue with cleaning the data?"
								+ " The original data won't be changed. This process can take some time."
								+ " If you cancel the cleaning of the data, the whole import process will be cancelled."
								+ "\n Y = Yes, continue with cleaning of data\n N = No, cancel import process", "Y, N", scanner);
					}
					if (isXmlCleanOk.equals("Y")) {
						// Start cleaning XML
						XmlCleaner xmlc = new XmlCleaner();
						boolean cleaningProcessDone = xmlc.cleanXml(pathToMabXmlFile);
						boolean isNewXmlFileClean = false;
						if (cleaningProcessDone == true) {
							pathToMabXmlFile = xmlc.getCleanedFile();
							isNewXmlFileClean = bxh.validateXML(xmlc.getCleanedFile());
							if (isNewXmlFileClean == false) {
								this.smHelper.print(this.print, "\nData could not be cleaned! Cancelled import process.");
								return;
							} else {
								hasValidationPassed = true;
							}
						} else {
							this.smHelper.print(this.print, "\nProblem with cleaning the data! Maybe you do not have write permissions"
									+ " to the folder, to which the cleaned data will be written. This is the same as the one of the original file: "
									+ pathToMabXmlFile);
							return;
						}
					} else {
						this.smHelper.print(this.print, "\nImport process cancelled as requested by user.");
						return;
					}
				}
			} else {

				hasValidationPassed = true;
			}


			if (hasValidationPassed) {
				if (isValidationOk.equals("V")) {
					this.smHelper.print(this.print, "\nValidation was successful.");
				}
				if (isValidationOk.equals("S")) {
					this.smHelper.print(this.print, "\nSkiped validation.");
				}

				if (!isWithCliArgs) {
					solrServerAddress = Main.getUserInput("\nSpecify the Solr Server address (URL) incl. core name (e. g. http://localhost:8080/solr/corename)", "solrPing", scanner);
				}
				if (!isWithCliArgs) {
					useDefaultMabPropertiesFile = Main.getUserInput("\nDo you want to use the default \"mab.properties\" file? "
							+ "If not, you can specify your own custom .properties file."
							+ "\n D = Default file\n C = Custom file", "D, C", scanner);
				}
				if (useDefaultMabPropertiesFile.equals("D")) {
					useDefaultMabProperties = true;
					pathToMabPropertiesFile = "/main/resources/mab.properties";
					directoryOfTranslationFiles = "/main/resources";
					propertiesFileInfo = "Use default mab.properties file";
				} else {
					useDefaultMabProperties = false;
					if (!isWithCliArgs) {
						pathToMabPropertiesFile = Main.getUserInput("\nSpecify a path to your own custom .properties file (e. g. /home/username/my.properties)."
								+ " Please be aware that the file suffix must be \".properties\".", "propertiesExists", scanner);
					}
					
					propertiesFileInfo = "Use custom .properties file: " + pathToMabPropertiesFile;

					directoryOfTranslationFiles = new File(pathToMabPropertiesFile).getParent();
					areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile, directoryOfTranslationFiles);
					
					// It the translation files, that are defined in the custom MAB properties file, do not exist
					// (they have to be in the same directory), that give an appropriate message:
					while (areTranslationFilesOk == false) {
						scanner.nextLine();
						areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile ,directoryOfTranslationFiles);
					}


				}

				if (!isWithCliArgs) {
					isIndexingOk = Main.getUserInput("\nEverything is ready now. Please review your choices:"
							+ "\n Data file:\t" + pathToMabXmlFile
							+ "\n Solr Server:\t" + solrServerAddress
							+ "\n .properties:\t" + propertiesFileInfo
							+ "\n\nDo you want to begin the import process?"
							+ "\nATTENTION: Depending on the amount of data and the performance of the computer, the import process could take quite some time."
							+ " So time to grab a coffee or a beer :-) "
							+ " \n Y = Yes, start import process\n N = No, cancel import process", "Y, N", scanner);
				}

				if (isIndexingOk.equals("Y")) {

					if (!isWithCliArgs) {
						this.optimize = true;
					}

					// Create SolrSever:
					solrServer = new HttpSolrServer(solrServerAddress);

					startTime = System.currentTimeMillis();
					
					// Index metadata so Solr
					if (indexSampleData) {
						Index index = new Index(true, this.solrServer, useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, true, true);
						isIndexingSuccessful = index.isIndexingSuccessful();
					} else {
						Index index = new Index(pathToMabXmlFile, this.solrServer, useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, false, this.print);
						isIndexingSuccessful = index.isIndexingSuccessful();
					}
					

					// Connect child and parent volumes:
					Relate relate = new Relate(this.solrServer, this.timeStamp, false, this.print);
					isRelateSuccessful = relate.isRelateSuccessful();
					
					if (this.optimize) {
						this.smHelper = new SolrMabHelper(solrServer);
						this.smHelper.print(this.print, "\nOptimizing Solr Server. That can take a while ...\n");
						this.smHelper.solrOptimize();
					}

					if (isIndexingSuccessful && isRelateSuccessful) {
						endTime = System.currentTimeMillis();
						this.smHelper.print(this.print, "\nDone importing. Everything successful. Execution time: " + this.smHelper.getExecutionTime(startTime, endTime) + "\n");
					} else {
						System.err.println("\nError while importing data.\n");
						return;
					}
				} else {
					this.smHelper.print(this.print, "\nImport process cancelled as requested by user.\n");
					return;
				}

			}
		} else {
			this.smHelper.print(this.print, "\nImport process cancelled as requested by user.");
			return;
		}

		if (!isWithCliArgs) {
			scanner.close();
		}
	}

}
