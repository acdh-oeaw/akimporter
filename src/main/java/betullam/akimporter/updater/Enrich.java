package main.java.betullam.akimporter.updater;

import java.io.File;
import java.util.Date;

import ak.xmlhelper.XmlMerger;
import main.java.betullam.akimporter.main.AkImporterHelper;

public class Enrich {

	private String enrichName;
	private boolean enrichDownload;
	private String enrichFtpHost;
	private String enrichFtpPort;
	private String enrichFtpUser; 
	private String enrichFtpPass; 
	private String enrichRemotePath;
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
			String enrichFtpUser, String enrichFtpPass, String enrichRemotePath, boolean enrichIsSftp,
			String enrichHostKey, String enrichLocalPath, boolean enrichUnpack, boolean enrichMerge,
			String enrichMergeTag, String enrichMergeLevel, String enrichMergeParentTag,
			String enrichProperties, String enrichSolr, boolean print, boolean optimize) {
		this.enrichName = enrichName;
		this.enrichDownload = enrichDownload;
		this.enrichFtpHost = enrichFtpHost;
		this.enrichFtpPort = enrichFtpPort;
		this.enrichFtpUser = enrichFtpUser;
		this.enrichFtpPass = enrichFtpPass;
		this.enrichRemotePath = enrichRemotePath;
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
		
		// Download data from given FTP server
		if (this.enrichDownload) {
			localPathOriginal = localPath + File.separator + "original" + File.separator + timeStamp;
			AkImporterHelper.mkDirIfNotExists(localPathOriginal);
			int enrichFtpPortInt = Integer.valueOf(this.enrichFtpPort);
			if (this.enrichIsSftp) {
				new FtpDownload().downloadFilesSftp(this.enrichRemotePath, localPathOriginal, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, this.enrichHostKey, this.print);
			} else {
				new FtpDownload().downloadFiles(this.enrichRemotePath, localPathOriginal, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, this.print);
			}
		}

		// Unpack tar.gz files
		if (this.enrichUnpack) {
			localPathExtracted = localPath + File.separator + "extracted" + File.separator + timeStamp;
			AkImporterHelper.mkDirIfNotExists(localPathExtracted);
			AkImporterHelper.print(print, "Extracting files to " + localPathExtracted + " ... ");
			ExtractTarGz etg = new ExtractTarGz();
			etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
			AkImporterHelper.print(print, "Done");
		}
		
		// Merge multiple xml files into one file
		if (this.enrichMerge) {
			localPathMerged = localPath + File.separator + "merged" + File.separator + timeStamp;
			AkImporterHelper.mkDirIfNotExists(localPathMerged);
			AkImporterHelper.print(print, "\nMerging extracted files to " + localPathMerged + File.separator + timeStamp + ".xml ... ");
			String pathToDestinationFile = localPathMerged + File.separator + timeStamp + ".xml";
			XmlMerger xmlm = new XmlMerger();
			int enrichMergeTagInt = Integer.valueOf(this.enrichMergeLevel);
			xmlm.mergeElements(localPathExtracted, pathToDestinationFile, this.enrichMergeParentTag, this.enrichMergeTag, enrichMergeTagInt);
		}
		
		// Start enrichment
		// TODO: Implement enrichment code - Atomic Solr Updates based on a .properties file.
	}

}
