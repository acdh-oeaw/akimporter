/**
 * Updating data changes.
 * Handling of data updates, e. g. on a daily basis with a cron job.
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
package main.java.betullam.akimporter.updater;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.solrmab.Index;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class Updater {
	
	private HttpSolrServer solrServer;
	boolean isUpdateSuccessful = false;
	String timeStamp;
	String localPathOriginal;
	String localPathExtracted;
	String localPathMerged;
	String pathToMabXmlFile;
	boolean useDefaultMabProperties;
	String pathToMabPropertiesFile;
	String directoryOfTranslationFiles;
	boolean hasValidationPassed;
	boolean isIndexingSuccessful;
	boolean print = false;
	boolean optimize = false;
	private SolrMabHelper smHelper;

	
	/**
	 * Handling the update process of ongoing data deliveries.
	 * 
	 * @param remotePath			Path to a directory on FTP server in which the file containing the updates is stored.
	 * @param localPath				Local path where the update file should be stored.
	 * @param host					Host name of FTP server where the updates are stored.
	 * @param port					Port of the FTP server.
	 * @param user					FTP username.
	 * @param password				FTP password.
	 * @param solrAddress			URL to Solr server incl. core name where the updates should be indexed to.
	 * @param ownMabProps			true if own custom mab.properties file should be used for indexing. False if the default should be used.
	 * @param pathToOwnMabProps		If using own custom mab.properties file: full path to it.
	 * @param optimize				true if Solr core should be optimized after update process.
	 * @param print					true if status messages should be printed to console.
	 * @return						true if update process was successful.
	 */ 
	public boolean update(String remotePath, String localPath, String host, int port, String user, String password, String solrAddress, boolean defaultMabProperties, String pathToCustomMabProps, boolean optimize, boolean print) {

		this.solrServer = new HttpSolrServer(solrAddress);
		this.timeStamp = String.valueOf(new Date().getTime());
		this.optimize = optimize;
		this.print = print;
		this.useDefaultMabProperties = (defaultMabProperties) ? true : false;
		this.pathToMabPropertiesFile = (defaultMabProperties) ? null : pathToCustomMabProps;
		this.smHelper = new SolrMabHelper(solrServer);
		
		this.smHelper.print(this.print, "\n-------------------------------------------");
		
		localPathOriginal = stripFileSeperatorFromPath(localPath) + File.separator + "original" + File.separator + timeStamp;
		localPathExtracted = stripFileSeperatorFromPath(localPath) + File.separator + "extracted" + File.separator + timeStamp;
		localPathMerged = stripFileSeperatorFromPath(localPath) + File.separator + "merged" + File.separator + timeStamp;
		mkDirIfNoExists(localPathOriginal);
		mkDirIfNoExists(localPathExtracted);
		mkDirIfNoExists(localPathMerged);
		
		this.smHelper.print(this.print, "\nUpdate starting: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(timeStamp))));
		
		FtpDownload ftpDownload = new FtpDownload();
		boolean isDownloadSuccessful = ftpDownload.downloadFiles(remotePath, localPathOriginal, host, port, user, password, this.print);
		
		if (isDownloadSuccessful) {
			
			// Extract downloaded .tar.gz file(s):
			this.smHelper.print(this.print, "Extracting downloaded files to "+localPathExtracted+" ... ");
			ExtractTarGz etg = new ExtractTarGz();
			etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
			this.smHelper.print(this.print, "Done");
			
			// Merge extracted files from downloaded .tar.gz file(se):
			this.smHelper.print(this.print, "\nMerging extracted files to "+localPathMerged + File.separator + timeStamp + ".xml ... ");
			pathToMabXmlFile = localPathMerged + File.separator + timeStamp + ".xml";
			XmlMerger xmlm = new XmlMerger();
			xmlm.mergeElementNodes(localPathExtracted, pathToMabXmlFile, "collection", "record", 1);
			this.smHelper.print(this.print, "Done");
			
			// Validate merged XML file:
			this.smHelper.print(this.print, "\nValidating merged file ... ");
			XmlValidator bxh = new XmlValidator();
			hasValidationPassed = bxh.validateXML(pathToMabXmlFile);
			this.smHelper.print(this.print, "Done");
			
			// Index XML file:
			if (hasValidationPassed) {
				if (this.useDefaultMabProperties) {
					//pathToMabPropertiesFile = "/betullam/akimporter/resources/mab.properties";
					//directoryOfTranslationFiles = "/betullam/akimporter/resources";
					pathToMabPropertiesFile = "/mab.properties";
					directoryOfTranslationFiles = "/";
					this.smHelper.print(this.print, "\nUse default mab.properties file for indexing.");
				} else {
					directoryOfTranslationFiles = new File(this.pathToMabPropertiesFile).getParent();
					this.smHelper.print(this.print, "\nUse custom mab.properties file for indexing: " + pathToMabPropertiesFile);
				}
				
				this.smHelper.print(this.print, "\nStart indexing ... ");
				
				// Index metadata so Solr
				Index index = new Index(pathToMabXmlFile, this.solrServer, this.useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, false, false);
				boolean isIndexingSuccessful = index.isIndexingSuccessful();

				if (isIndexingSuccessful) {
					this.smHelper.print(this.print, "Done");
				}
				
				this.smHelper.print(this.print, "\nStart linking parent and child records ... ");
				
				// Connect child and parent volumes:
				Relate relate = new Relate(this.solrServer, this.timeStamp, false, false);
				boolean isRelateSuccessful = relate.isRelateSuccessful();
				
				if (isRelateSuccessful) {
					this.smHelper.print(this.print, "Done");
				}
				
				if (this.optimize) {
					this.smHelper.print(this.print, "\nOptimizing Solr Server ... ");
					this.smHelper.solrOptimize();
					this.smHelper.print(this.print, "Done");
				}

				
				if (isIndexingSuccessful && isRelateSuccessful) {
					this.smHelper.print(this.print, "\nEVERYTHING WAS SUCCESSFUL!");
					isUpdateSuccessful = true;
				} else {
					this.smHelper.print(this.print, "\nError while importing!\n");
					isUpdateSuccessful = false;
				}
				 
			} else {
				isUpdateSuccessful = false;
			}
			
		} else {
			isUpdateSuccessful = false;
		}
		
		this.smHelper.print(this.print, "\n-------------------------------------------");
		return isUpdateSuccessful;
		
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
	
	/**
	 * Creates a directory if it does not exist.
	 * 
	 * @param path	Path to the directory that should be created.
	 */
	private static void mkDirIfNoExists(String path) {
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

}
