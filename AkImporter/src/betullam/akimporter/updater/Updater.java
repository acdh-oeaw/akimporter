package betullam.akimporter.updater;

import java.io.File;
import java.util.Date;

import betullam.akimporter.main.Main;
import betullam.akimporter.solrmab.SolrMab;
import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlValidator;

public class Updater {
	
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
	boolean print = true;
	
	
	public boolean update(String remotePath, String localPath, String host, int port, String user, String password, String solrAddress, String defaultSolrMab, boolean showMessages) {

		print = showMessages;
		timeStamp = String.valueOf(new Date().getTime());
		localPathOriginal = stripFileSeperatorFromPath(localPath) + File.separator + "original" + File.separator + timeStamp;
		localPathExtracted = stripFileSeperatorFromPath(localPath) + File.separator + "extracted" + File.separator + timeStamp;
		localPathMerged = stripFileSeperatorFromPath(localPath) + File.separator + "merged" + File.separator + timeStamp;
		mkDirIfNoExists(localPathOriginal);
		mkDirIfNoExists(localPathExtracted);
		mkDirIfNoExists(localPathMerged);
		
		
		FtpDownload ftpDownload = new FtpDownload();
		boolean isDownloadSuccessful = ftpDownload.downloadFiles(remotePath, localPathOriginal, host, port, user, password, showMessages);
		
		if (isDownloadSuccessful) {
			
			// Extract downloaded .tar.gz file(s):
			print("Extracting downloaded files ...");
			ExtractTarGz etg = new ExtractTarGz();
			etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
			print("Done extracting.");
			
			// Merge extracted files from downloaded .tar.gz file(se):
			print("Merging extracted files ...");
			pathToMabXmlFile = localPathMerged + File.separator + timeStamp + ".xml";
			XmlMerger xmlm = new XmlMerger();
			xmlm.mergeElementNodes(localPathExtracted, pathToMabXmlFile, "collection", "record", 1);
			print("Done merging.");
			
			// Validate merged XML file:
			print("Validate merged file ...");
			XmlValidator bxh = new XmlValidator();
			hasValidationPassed = bxh.validateXML(pathToMabXmlFile);
			print("Done validating");
			
			// Index XML file:
			if (hasValidationPassed) {
				
				if (defaultSolrMab.equals("default")) {
					useDefaultMabProperties = true;
					pathToMabPropertiesFile = Main.class.getResource("/betullam/akimporter/resources/mab.properties").getFile();
					directoryOfTranslationFiles = Main.class.getResource("/betullam/akimporter/resources").getPath();
					print("Use default mab.properties file for indexing.");
				} else {
					useDefaultMabProperties = false;
					pathToMabPropertiesFile = defaultSolrMab;
					directoryOfTranslationFiles = new File(pathToMabPropertiesFile).getParent();
					print("Use custom mab.properties file for indexing: " + pathToMabPropertiesFile);
				}
				
				print("Start indexing ...");
				SolrMab sm = new SolrMab(false);
				isIndexingSuccessful = sm.startIndexing(pathToMabXmlFile, solrAddress, pathToMabPropertiesFile, directoryOfTranslationFiles, useDefaultMabProperties);
	
				if (isIndexingSuccessful == true) {
					print("Done indexing.\nEVERYTHING WAS SUCCESSFUL!");
					isUpdateSuccessful = true;
				} else {
					System.out.println("\nFehler beim Import-Vorgang!\n");
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
	
	private void print(String text) {
		if (print) {
			System.out.println(text);
		}
	}

}
