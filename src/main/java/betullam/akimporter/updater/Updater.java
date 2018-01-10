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

import ak.xmlhelper.XmlMerger;
import ak.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.Index;
import main.java.betullam.akimporter.solrmab.Relate;
import main.java.betullam.akimporter.solrmab.relations.AuthorityFlag;
import main.java.betullam.akimporter.solrmab.relations.AuthorityMerge;

public class Updater {

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
	public boolean update(String remotePath, String remotePathMoveTo, String localPath, String host, int port, String hostKey, String user, String password, String solrServerAddrBiblio, String solrServerAddrAuth, boolean defaultMabProperties, String pathToCustomMabProps, String entities, boolean authFlagOnly, boolean authMerge, boolean optimize, boolean print) {

		// Setting variables:
		boolean isUpdateSuccessful = false;
		HttpSolrServer solrServerBiblio = (solrServerAddrBiblio != null && !solrServerAddrBiblio.isEmpty()) ? new HttpSolrServer(solrServerAddrBiblio) : null;
		HttpSolrServer solrServerAuth = (solrServerAddrAuth != null && !solrServerAddrAuth.isEmpty()) ? new HttpSolrServer(solrServerAddrAuth) : null;
		String timeStamp = String.valueOf(new Date().getTime());
		boolean useDefaultMabProperties = (defaultMabProperties) ? true : false;
		String pathToMabPropertiesFile = (defaultMabProperties) ? null : pathToCustomMabProps;
		String localPathOriginal = stripFileSeperatorFromPath(localPath) + File.separator + "original" + File.separator + timeStamp;
		String localPathExtracted = stripFileSeperatorFromPath(localPath) + File.separator + "extracted" + File.separator + timeStamp;
		String localPathMerged = stripFileSeperatorFromPath(localPath) + File.separator + "merged" + File.separator + timeStamp;
		mkDirIfNoExists(localPathOriginal);
		mkDirIfNoExists(localPathExtracted);
		mkDirIfNoExists(localPathMerged);

		AkImporterHelper.print(print, "\n-------------------------------------------");
		AkImporterHelper.print(print, "\nUpdate starting: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(timeStamp))));

		boolean isDownloadSuccessful = false;
		FtpDownload ftpDownload = new FtpDownload();
		if (port == 22) {
			isDownloadSuccessful = new FtpDownload().downloadFilesSftp(remotePath, remotePathMoveTo, localPathOriginal, host, port, user, password, hostKey, timeStamp, print);
		} else {
			isDownloadSuccessful = ftpDownload.downloadFiles(remotePath, remotePathMoveTo, localPathOriginal, host, port, user, password, timeStamp, false, print);
		}
		
		if (isDownloadSuccessful) {

			// Extract downloaded .tar.gz file(s):
			AkImporterHelper.print(print, "\nExtracting downloaded files to "+localPathExtracted+" ... ");
			ExtractTarGz etg = new ExtractTarGz();
			etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
			AkImporterHelper.print(print, "Done");

			// Merge extracted files from downloaded .tar.gz file(se):
			AkImporterHelper.print(print, "\nMerging extracted files to "+localPathMerged + File.separator + timeStamp + ".xml ... ");
			String pathToMabXmlFile = localPathMerged + File.separator + timeStamp + ".xml";
			XmlMerger xmlm = new XmlMerger();
			xmlm.mergeElements(localPathExtracted, pathToMabXmlFile, "collection", "record", 1);
			AkImporterHelper.print(print, "Done");

			// Validate merged XML file:
			AkImporterHelper.print(print, "\nValidating merged file ... ");
			XmlValidator bxh = new XmlValidator();
			boolean hasValidationPassed = bxh.validateXML(pathToMabXmlFile);
			AkImporterHelper.print(print, "Done");

			// Index XML file:
			if (hasValidationPassed) {
				String directoryOfTranslationFiles = null;
				if (useDefaultMabProperties) {
					// IMPORTANT: To use "/main/resources/..." we have to make sure that this folder is exported to the right path within the .jar
					// file. You can find this out when opening the .jar file with an Archive reader. To change pathes when exporting to .jar, go
					// (in Eclipse) to "Project Properties -> Java Build Path -> Source-Tab -> Select "Included" and "Edit ..." -> Add files or folders"
					// When using folders, don't forget to use wildcards.
					pathToMabPropertiesFile = "/main/resources/mab.properties";
					directoryOfTranslationFiles = "/main/resources";
					AkImporterHelper.print(print, "\nUse default mab.properties file for indexing.");
				} else {
					directoryOfTranslationFiles = new File(pathToMabPropertiesFile).getParent();
					AkImporterHelper.print(print, "\nUse custom mab.properties file for indexing: " + pathToMabPropertiesFile);
				}

				AkImporterHelper.print(print, "\nStart indexing ... ");

				// Index metadata so Solr
				Index index = new Index(pathToMabXmlFile, solrServerBiblio, useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, timeStamp, false, false);
				boolean isIndexingSuccessful = index.isIndexingSuccessful();

				if (isIndexingSuccessful) {
					AkImporterHelper.print(print, "Done");
				}

				AkImporterHelper.print(print, "\nStart linking parent and child records ... ");

				// Connect child and parent volumes:
				Relate relate = new Relate(solrServerBiblio, timeStamp, false, false);
				boolean isRelateSuccessful = relate.isRelateSuccessful();

				if (isRelateSuccessful) {
					AkImporterHelper.print(print, "Done");
				}

				if (authFlagOnly) {
					AkImporterHelper.print(print, "\nStart setting flags of existance to authority records ... ");
					AuthorityFlag af = new AuthorityFlag(solrServerBiblio, solrServerAuth, timeStamp, false, false);
					af.setFlagOfExistance();
					AkImporterHelper.print(print, "Done");
				}

				if (authMerge) {
					AkImporterHelper.print(print, "\nStart merging authority records to bibliographic records ... ");
					// If -f is not set, we should set flag of existance to authority anyway!
					if (!authFlagOnly) {
						AkImporterHelper.print(print, "\nStart setting flags of existance to authority records ... ");
						AuthorityFlag af = new AuthorityFlag(solrServerBiblio, solrServerAuth, timeStamp, false, false);
						af.setFlagOfExistance();
						AkImporterHelper.print(print, "Done");
					}
					AuthorityMerge ai = new AuthorityMerge(solrServerBiblio, solrServerAuth, timeStamp, false, false);
					ai.mergeAuthorityToBiblio(entities);
					AkImporterHelper.print(print, "Done");
				}

				if (optimize) {
					AkImporterHelper.print(print, "\nOptimizing Solr Server ... ");
					AkImporterHelper.solrOptimize(solrServerBiblio);
					AkImporterHelper.print(print, "Done");
				}


				if (isIndexingSuccessful && isRelateSuccessful) {
					AkImporterHelper.print(print, "\nEVERYTHING WAS SUCCESSFUL!");
					isUpdateSuccessful = true;
				} else {
					AkImporterHelper.print(print, "\nError while importing!\n");
					isUpdateSuccessful = false;
				}

			} else {
				isUpdateSuccessful = false;
			}

		} else {
			isUpdateSuccessful = false;
		}

		AkImporterHelper.print(print, "\n-------------------------------------------");
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
