/**
 * Consolidates multiple XML files to a new updated one.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlSplitter;
import main.java.betullam.akimporter.solrmab.SolrMabHelper;

public class Consolidate {

	private SolrMabHelper smHelper = new SolrMabHelper();
	private String pathToInitialDataset = null;
	private String pathToUpdateDir = null;
	private String pathToConsolidatedFile = null;
	private boolean print = false;

	/**
	 * Constructor for setting some variables and starting the consolidation process.
	 * 
	 * @param pathToInitialDataset		String: Path to the initial dataset of all data
	 * @param pathToUpdateDir			String: Path to the update directory for ongoing updates
	 * @param pathToConsolidatedFile	String: Path to where the new consolidated file should be saved
	 * @param print						boolean indicating if status messages should be print
	 */
	public Consolidate(String pathToInitialDataset, String pathToUpdateDir, String pathToConsolidatedFile, boolean print) {
		this.pathToInitialDataset = pathToInitialDataset;
		this.pathToUpdateDir = pathToUpdateDir;
		this.pathToConsolidatedFile = pathToConsolidatedFile;
		this.print = print;
		this.start();
	}

	/**
	 * Start the consolidation process (used in class constructor)
	 */
	private void start() {

		List<File> filesForSplitting = new ArrayList<File>();
		if (this.pathToUpdateDir != null && !this.pathToUpdateDir.isEmpty()) {
			// Get a sorted list (oldest to newest) from all ongoing data deliveries:
			File fPathToMergedDir = new File(stripFileSeperatorFromPath(this.pathToUpdateDir) + File.separator + "merged");
			filesForSplitting = (List<File>)FileUtils.listFiles(fPathToMergedDir, new String[] {"xml"}, true); // Get all xml-files recursively
			Collections.sort(filesForSplitting); // Sort oldest to newest
		}

		// Add the initial dataset at the first position of the file list
		File initialDataset = new File(pathToInitialDataset);
		filesForSplitting.add(0, initialDataset);

		XmlSplitter xmls = new XmlSplitter(null);
		String pathToSplittedFilesDir = xmls.getDestinationDirectory().getAbsolutePath();

		// Split XML. Older records will be overwritten by newer records:
		for (File fileForSplitting : filesForSplitting) {
			this.smHelper.print(this.print, "\nSplitting file " + fileForSplitting.getAbsolutePath() + " ...");
			Map<String, String> condAttrs = new HashMap<String, String>();
			condAttrs.put("tag", "SYS");
			xmls.split(fileForSplitting.getAbsolutePath(), "record", 0, "controlfield", condAttrs);
		}

		// Merge XML:
		XmlMerger xmlm = new XmlMerger(); // Start merging
		boolean isMergingSuccessful = xmlm.mergeElementNodes(pathToSplittedFilesDir, this.pathToConsolidatedFile, "collection", "record", 0);

		if (isMergingSuccessful) {
			this.smHelper.print(this.print, "\nConsolidating data into file " + this.pathToConsolidatedFile + " was successful.\n");
		} else {
			System.err.println("\nError while consolidating! Cancelled process.\n");
			return;
		}


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