package betullam.akimporter.main;

import java.io.File;
import java.util.Date;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.akimporter.solrmab.Index;

public class Authority {

	private String pathToAuthFile = null;
	private boolean useDefaultAuthProperties = true;
	private String pathToAuthProperties = null;
	private String pathToTranslationFiles = null;
	private String solrServer = null;
	private String timeStamp = null;
	private boolean print = false;
	private boolean optimize = false;
	//private SolrMabHelper smHelper = new SolrMabHelper();
	
	
	public Authority(String pathToAuthFile, boolean useDefaultAuthProperties, String pathToCustomAuthProperties, String solrServer, String timeStamp, boolean print, boolean optimize) {
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
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;
		this.optimize = optimize;
	}
	
	public void indexAuthority() {
		HttpSolrServer solrServerAuth = new HttpSolrServer(this.solrServer);
		if (this.timeStamp == null) {
			this.timeStamp = String.valueOf(new Date().getTime());
		}

		new Index (
				this.pathToAuthFile,
				solrServerAuth,
				this.useDefaultAuthProperties,
				this.pathToAuthProperties,
				this.pathToTranslationFiles,
				this.timeStamp,
				this.optimize,
				this.print
		);
	}
}
