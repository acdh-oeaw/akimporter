/**
 * Importing authority records (GND) to Solr index.
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

package betullam.akimporter.main;

import java.io.File;
import java.util.Date;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.akimporter.solrmab.Index;
import betullam.akimporter.solrmab.SolrMabHelper;
import betullam.akimporter.solrmab.relations.AuthorityFlag;

public class Authority {

	private String pathToAuthFile = null;
	private boolean useDefaultAuthProperties = true;
	private String pathToAuthProperties = null;
	private String pathToTranslationFiles = null;
	private String solrServerAuth = null;
	private String solrServerBiblio = null;
	private String timeStamp = null;
	private boolean print = false;
	private boolean optimize = false;
	private SolrMabHelper smHelper = new SolrMabHelper();
	
	/**
	 * Constructor for setting some variables.
	 * 
	 * @param pathToAuthFile				String indicating the path to an authority file (e. g. /path/to/persons.xml)
	 * @param useDefaultAuthProperties		boolean indicating if the default authority properties for indexing should be used
	 * @param pathToCustomAuthProperties	String indicating the path to a custom .properties file (e. g. /path/to/custom_authority.properties)
	 * @param solrServerAuth				String indicating the URL incl. core name of the Solr authority index (e. g. http://localhost:8080/solr/authority)
	 * @param solrServerBiblio				String indicating the URL incl. core name of the Solr bibliographic index (e. g. http://localhost:8080/solr/biblio)
	 * @param timeStamp						Current unix time stamp as a String or null
	 * @param print							boolean indicating whether to print status messages or not
	 * @param optimize						boolean indicating whether to optimize the solr index not
	 */
	public Authority(String pathToAuthFile, boolean useDefaultAuthProperties, String pathToCustomAuthProperties, String solrServerAuth, String solrServerBiblio, String timeStamp, boolean print, boolean optimize) {
		this.pathToAuthFile = pathToAuthFile;
		this.useDefaultAuthProperties = useDefaultAuthProperties;
		if (this.useDefaultAuthProperties) {
			this.pathToAuthProperties = "/betullam/akimporter/resources/authority.properties";
			this.pathToTranslationFiles = "/betullam/akimporter/resources";
		} else {
			this.pathToAuthProperties = pathToCustomAuthProperties;
			this.pathToTranslationFiles = new File(this.pathToAuthProperties).getParent();
			
			// It the translation files, that are defined in the custom authority properties file, do not exist
			// (they have to be in the same directory), that give an appropriate message:
			boolean areTranslationFilesOk = Main.translationFilesExist(this.pathToAuthProperties, this.pathToTranslationFiles);
			if (areTranslationFilesOk == false) {
				System.err.println("Stopping import process due to error with translation files.");
			}
		}
		this.solrServerAuth = solrServerAuth;
		this.solrServerBiblio = solrServerBiblio;
		this.timeStamp = timeStamp;
		this.print = print;
		this.optimize = optimize;
	}
	
	/**
	 * Starting the index process for authority records.
	 * @return	true if the index process was sucessful, false otherwise
	 */
	public boolean indexAuthority() {
		
		boolean returnValue = false;
		
		HttpSolrServer solrServerAuth = new HttpSolrServer(this.solrServerAuth);
		if (this.timeStamp == null) {
			this.timeStamp = String.valueOf(new Date().getTime());
		}

		Index index = new Index (
						this.pathToAuthFile,
						solrServerAuth,
						this.useDefaultAuthProperties,
						this.pathToAuthProperties,
						this.pathToTranslationFiles,
						this.timeStamp,
						this.optimize,
						this.print
		);
		
		if(index.isIndexingSuccessful()) {;
			HttpSolrServer solrServerBiblio = new HttpSolrServer(this.solrServerBiblio);
			AuthorityFlag af = new AuthorityFlag(solrServerBiblio, solrServerAuth, null, print);
			af.setFlagOfExistance();
			this.smHelper.print(this.print, "\nDone indexing authority records.");
			
			returnValue = true;
		} else {
			System.err.println("Error indexing authority records!");
			returnValue = false;
		}
		
		return returnValue;
	}
}
