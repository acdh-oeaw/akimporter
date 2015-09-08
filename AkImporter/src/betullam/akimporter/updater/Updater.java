package betullam.akimporter.updater;

import java.io.File;
import java.util.Date;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.akimporter.main.Main;
import betullam.akimporter.solrmab.Index;
import betullam.akimporter.solrmab.Relate;
import betullam.akimporter.solrmab.SolrMabHelper;
import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlValidator;

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

	
	public boolean update(String remotePath, String localPath, String host, int port, String user, String password, String solrAddress, boolean ownMabProps, String pathToOwnMabProps, boolean optimize, boolean print) {

		this.solrServer = new HttpSolrServer(solrAddress);
		this.timeStamp = String.valueOf(new Date().getTime());
		this.optimize = optimize;
		this.print = print;
		this.useDefaultMabProperties = (ownMabProps) ? false : true;
		this.pathToMabPropertiesFile = (ownMabProps) ? pathToOwnMabProps : null;
		this.smHelper = new SolrMabHelper(solrServer);
		
		localPathOriginal = stripFileSeperatorFromPath(localPath) + File.separator + "original" + File.separator + timeStamp;
		localPathExtracted = stripFileSeperatorFromPath(localPath) + File.separator + "extracted" + File.separator + timeStamp;
		localPathMerged = stripFileSeperatorFromPath(localPath) + File.separator + "merged" + File.separator + timeStamp;
		mkDirIfNoExists(localPathOriginal);
		mkDirIfNoExists(localPathExtracted);
		mkDirIfNoExists(localPathMerged);
		
		
		FtpDownload ftpDownload = new FtpDownload();
		boolean isDownloadSuccessful = ftpDownload.downloadFiles(remotePath, localPathOriginal, host, port, user, password, this.print);
		
		if (isDownloadSuccessful) {
			
			// Extract downloaded .tar.gz file(s):
			this.smHelper.print(this.print, "Extracting downloaded files ...\r");
			ExtractTarGz etg = new ExtractTarGz();
			etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
			this.smHelper.print(this.print, "Extracting downloaded files ... Done");
			
			// Merge extracted files from downloaded .tar.gz file(se):
			this.smHelper.print(this.print, "\nMerging extracted files ...\r");
			pathToMabXmlFile = localPathMerged + File.separator + timeStamp + ".xml";
			XmlMerger xmlm = new XmlMerger();
			xmlm.mergeElementNodes(localPathExtracted, pathToMabXmlFile, "collection", "record", 1);
			this.smHelper.print(this.print, "Merging extracted files ... Done");
			
			// Validate merged XML file:
			this.smHelper.print(this.print, "\nValidate merged file ...\r");
			XmlValidator bxh = new XmlValidator();
			hasValidationPassed = bxh.validateXML(pathToMabXmlFile);
			this.smHelper.print(this.print, "Validate merged file ... Done");
			
			// Index XML file:
			if (hasValidationPassed) {
				if (this.useDefaultMabProperties) {
					pathToMabPropertiesFile = Main.class.getResource("/betullam/akimporter/resources/mab.properties").getFile();
					directoryOfTranslationFiles = Main.class.getResource("/betullam/akimporter/resources").getPath();
					this.smHelper.print(this.print, "\nUse default mab.properties file for indexing.");
				} else {
					directoryOfTranslationFiles = new File(this.pathToMabPropertiesFile).getParent();
					this.smHelper.print(this.print, "\nUse custom mab.properties file for indexing: " + pathToMabPropertiesFile);
				}
				
				this.smHelper.print(this.print, "\nStart importing ...");
				
				// Index metadata so Solr
				Index index = new Index(pathToMabXmlFile, this.solrServer, this.useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, false, this.print);
				boolean isIndexingSuccessful = index.isIndexingSuccessful();

				// Connect child and parent volumes:
				Relate relate = new Relate(this.solrServer, this.timeStamp, false, this.print);
				boolean isRelateSuccessful = relate.isRelateSuccessful();
				
				if (this.optimize) {
					this.smHelper.print(this.print, "\nOptimizing Solr Server ...\n");
					this.smHelper.solrOptimize();
				}

				
				if (isIndexingSuccessful && isRelateSuccessful) {
					this.smHelper.print(this.print, "\nDone importing.\nEVERYTHING WAS SUCCESSFUL!");
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
		
		return isUpdateSuccessful;
		
	}
	
	
	private static String stripFileSeperatorFromPath(String path) {
		if (!path.equals(File.separator) && (path.length() > 0) && (path.charAt(path.length()-1) == File.separatorChar)) {
			path = path.substring(0, path.length()-1);
		}
		return path;
	}
	
	private static void mkDirIfNoExists(String path) {
		File dir = new File(path);
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

}
