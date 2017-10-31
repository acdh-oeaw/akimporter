package main.java.betullam.akimporter.updater;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Date;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import ak.xmlhelper.XmlMerger;
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.Index;

public class Enrich {

	//private String enrichName;
	private boolean enrichDownload;
	private String enrichFtpHost;
	private String enrichFtpPort;
	private String enrichFtpUser; 
	private String enrichFtpPass; 
	private String enrichRemotePath;
	private String enrichRemotePathMoveTo;
	private boolean enrichIsSftp;
	private String enrichHostKey;
	private String enrichLocalPath; 
	private boolean enrichUnpack; 
	private boolean enrichMerge;
	private String enrichMergeTag;
	private String enrichMergeLevel;
	private String enrichMergeParentTag;
	private String enrichProperties; 
	private String enrichSolr;
	private boolean print;
	private boolean optimize;

	
	public Enrich(String enrichName, boolean enrichDownload, String enrichFtpHost, String enrichFtpPort,
			String enrichFtpUser, String enrichFtpPass, String enrichRemotePath, String enrichRemotePathMoveTo,
			boolean enrichIsSftp, String enrichHostKey, String enrichLocalPath, boolean enrichUnpack,
			boolean enrichMerge, String enrichMergeTag, String enrichMergeLevel, String enrichMergeParentTag,
			String enrichProperties, String enrichSolr, boolean print, boolean optimize) {
		//this.enrichName = enrichName;
		this.enrichDownload = enrichDownload;
		this.enrichFtpHost = enrichFtpHost;
		this.enrichFtpPort = enrichFtpPort;
		this.enrichFtpUser = enrichFtpUser;
		this.enrichFtpPass = enrichFtpPass;
		this.enrichRemotePath = enrichRemotePath;
		this.enrichRemotePathMoveTo = enrichRemotePathMoveTo;
		this.enrichIsSftp = enrichIsSftp;
		this.enrichHostKey = enrichHostKey;
		this.enrichLocalPath = enrichLocalPath;
		this.enrichUnpack = enrichUnpack;
		this.enrichMerge = enrichMerge;
		this.enrichMergeTag = enrichMergeTag;
		this.enrichMergeLevel = enrichMergeLevel;
		this.enrichMergeParentTag = enrichMergeParentTag;
		this.enrichProperties = enrichProperties;
		this.enrichSolr = enrichSolr;
		this.print = print;
		this.optimize = optimize;

		this.enrich();
	};

	
	private void enrich() {
		String timeStamp = String.valueOf(new Date().getTime());
		String localPath = AkImporterHelper.stripFileSeperatorFromPath(this.enrichLocalPath);
		String localPathOriginal = localPath;
		String localPathExtracted = localPath;
		String localPathMerged = localPath;
		String pathToEnrichFile = localPath;
		
		// Download data from given FTP server
		if (this.enrichDownload) {
			localPathOriginal = localPath + File.separator + "original" + File.separator + timeStamp;
			AkImporterHelper.mkDirIfNotExists(localPathOriginal);
			int enrichFtpPortInt = Integer.valueOf(this.enrichFtpPort);
			if (this.enrichIsSftp) {
				new FtpDownload().downloadFilesSftp(this.enrichRemotePath, this.enrichRemotePathMoveTo, localPathOriginal, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, this.enrichHostKey, timeStamp, this.print);
			} else {
				new FtpDownload().downloadFiles(this.enrichRemotePath, this.enrichRemotePathMoveTo, localPathOriginal, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, timeStamp, this.print);
			}
			
			// Check if at least one XML or .tar.gz file was downloaded
			File downloadDestination = new File(localPathOriginal);
			File[] filesInDownloadDest = downloadDestination.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return (name.toLowerCase().endsWith(".xml") || name.toLowerCase().endsWith(".tar.gz"));
				}
			});
			
			// If no XML or .tar.gz file was downloaded, remove the (emtpy and therefore) useless directory
			if (filesInDownloadDest.length == 0) {
				downloadDestination.delete();
			}
			
			// Check also if the "original" directory is empty. If yes, delete it
			File origDirectory = new File(localPath + File.separator + "original");
			File[] filesInOrigDirectory = origDirectory.listFiles();
			if (filesInOrigDirectory.length == 0) {
				origDirectory.delete();
			}
		}

		// Unpack .tar.gz files
		if (this.enrichUnpack) {
			localPathExtracted = localPath + File.separator + "extracted" + File.separator + timeStamp;
			
			// Check if there is at least one .tar.gz file to unpack
			File unpackSourceDir = new File(localPathOriginal);
			File[] filesInUnpackSourceDir = (unpackSourceDir.exists()) ? unpackSourceDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.toLowerCase().endsWith(".tar.gz");
				}
			}) : null;
			
			// Unpack files but only if we have files we can unpack
			if (filesInUnpackSourceDir != null && filesInUnpackSourceDir.length > 0) {
				AkImporterHelper.mkDirIfNotExists(localPathExtracted);
				AkImporterHelper.print(print, "Extracting files to " + localPathExtracted + " ... ");
				ExtractTarGz etg = new ExtractTarGz();
				etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
				AkImporterHelper.print(print, "Done");
			}
		}
		
		// Merge multiple xml files into one file
		if (this.enrichMerge) {
			// Check if there is at least one XML file to merge
			File mergeSourceDir = new File(localPathExtracted);
			File[] filesInMergeSourceDir = (mergeSourceDir.exists()) ? mergeSourceDir.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.toLowerCase().endsWith(".xml");
				}
			}) : null;
			
			// Merge files but only if we have files we can merge
			if (filesInMergeSourceDir != null && filesInMergeSourceDir.length > 0) {
				localPathMerged = localPath + File.separator + "merged" + File.separator + timeStamp;
				AkImporterHelper.mkDirIfNotExists(localPathMerged);
				AkImporterHelper.print(print, "\nMerging extracted files to " + localPathMerged + File.separator + timeStamp + ".xml ... ");
				pathToEnrichFile = localPathMerged + File.separator + timeStamp + ".xml";
				XmlMerger xmlm = new XmlMerger();
				int enrichMergeTagInt = Integer.valueOf(this.enrichMergeLevel);
				xmlm.mergeElements(localPathExtracted, pathToEnrichFile, this.enrichMergeParentTag, this.enrichMergeTag, enrichMergeTagInt);
			}
		}
		
		// Start enrichment, but only if the enrich file exists and is an XML file
		File enrichFile = new File(pathToEnrichFile);
		boolean enrichFileIsXml = (enrichFile.exists() && enrichFile.isFile() && enrichFile.getName().toLowerCase().endsWith(".xml"));
		if (enrichFileIsXml) {
			String directoryOfTranslationFiles = new File(this.enrichProperties).getParent();
			HttpSolrServer enrichSolrServer = (this.enrichSolr != null && !this.enrichSolr.isEmpty()) ? new HttpSolrServer(this.enrichSolr) : null;
			new Index(pathToEnrichFile, enrichSolrServer, this.enrichProperties, directoryOfTranslationFiles, timeStamp, this.optimize, this.print);
		}
	}

}
