package main.java.betullam.akimporter.updater;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

import ak.xmlhelper.XmlMerger;
import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.Index;

public class Enrich {

	private String enrichName;
	private boolean enrichDownload;
	private String enrichFtpHost;
	private String enrichFtpPort;
	private String enrichFtpUser; 
	private String enrichFtpPass; 
	private String enrichRemotePath;
	private String enrichRemotePathMoveTo;
	private boolean enrichIsSftp;
	private String enrichHostKey;
	private String enrichLocalPathInitial;
	private String enrichLocalPathOngoing;
	private boolean enrichUnpack; 
	private boolean enrichMerge;
	private String enrichMergeTag;
	private String enrichMergeLevel;
	private String enrichMergeParentTag;
	private String enrichProperties; 
	private String enrichSolr;
	private boolean reimport;
	private boolean print;
	private boolean optimize;

	
	public Enrich(String enrichName, boolean enrichDownload, String enrichFtpHost, String enrichFtpPort,
			String enrichFtpUser, String enrichFtpPass, String enrichRemotePath, String enrichRemotePathMoveTo,
			boolean enrichIsSftp, String enrichHostKey, String enrichLocalPathInitial, String enrichLocalPathOngoing,
			boolean enrichUnpack, boolean enrichMerge, String enrichMergeTag, String enrichMergeLevel,
			String enrichMergeParentTag, String enrichProperties, String enrichSolr, boolean reimport, boolean print,
			boolean optimize) {
		this.enrichName = enrichName;
		this.enrichDownload = enrichDownload;
		this.enrichFtpHost = enrichFtpHost;
		this.enrichFtpPort = enrichFtpPort;
		this.enrichFtpUser = enrichFtpUser;
		this.enrichFtpPass = enrichFtpPass;
		this.enrichRemotePath = enrichRemotePath;
		this.enrichRemotePathMoveTo = enrichRemotePathMoveTo;
		this.enrichIsSftp = enrichIsSftp;
		this.enrichHostKey = enrichHostKey;
		this.enrichLocalPathInitial = enrichLocalPathInitial;
		this.enrichLocalPathOngoing = enrichLocalPathOngoing;
		this.enrichUnpack = enrichUnpack;
		this.enrichMerge = enrichMerge;
		this.enrichMergeTag = enrichMergeTag;
		this.enrichMergeLevel = enrichMergeLevel;
		this.enrichMergeParentTag = enrichMergeParentTag;
		this.enrichProperties = enrichProperties;
		this.enrichSolr = enrichSolr;
		this.reimport = reimport;
		this.print = print;
		this.optimize = optimize;

		this.enrich();
	};

	
	private void enrich() {
		String timeStamp = String.valueOf(new Date().getTime());
		String localPath = AkImporterHelper.stripFileSeperatorFromPath(this.enrichLocalPathOngoing);
		String localPathOriginal = localPath;
		String localPathExtracted = localPath;
		String localPathMerged = localPath;
		String pathToEnrichFile = this.enrichLocalPathInitial;
		String directoryOfTranslationFiles = new File(this.enrichProperties).getParent();
		HttpSolrServer enrichSolrServer = (this.enrichSolr != null && !this.enrichSolr.isEmpty()) ? new HttpSolrServer(this.enrichSolr) : null;
		
		// Download data from given FTP server
		if (this.enrichDownload && !this.reimport) {
			localPathOriginal = localPath + File.separator + "original" + File.separator + timeStamp;
			AkImporterHelper.mkDirIfNotExists(localPathOriginal);
			int enrichFtpPortInt = Integer.valueOf(this.enrichFtpPort);
			if (this.enrichIsSftp) {
				new FtpDownload().downloadFilesSftp(this.enrichRemotePath, this.enrichRemotePathMoveTo, localPathOriginal, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, this.enrichHostKey, timeStamp, this.print);
			} else {
				new FtpDownload().downloadFiles(this.enrichRemotePath, this.enrichRemotePathMoveTo, localPathOriginal, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, timeStamp, false, this.print);
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
				
				// Set the pathToEnrichFile variable to null because otherwise, the initial file would be taken
				pathToEnrichFile = null;
			}
			
			// Check also if the "original" directory is empty. If yes, delete it
			File origDirectory = new File(localPath + File.separator + "original");
			File[] filesInOrigDirectory = origDirectory.listFiles();
			if (filesInOrigDirectory.length == 0) {
				origDirectory.delete();
			}
		}

		// Unpack .tar.gz files
		if (this.enrichUnpack && !this.reimport) {
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
				AkImporterHelper.print(print, "\nExtracting files to " + localPathExtracted + " ... ");
				ExtractTarGz etg = new ExtractTarGz();
				etg.extractTarGz(localPathOriginal, timeStamp, localPathExtracted);
				AkImporterHelper.print(print, "Done");
			}
		}
		
		// Merge multiple xml files into one file
		if (this.enrichMerge && !this.reimport) {
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
				xmlm.mergeElements(localPathExtracted, pathToEnrichFile, this.enrichMergeParentTag, this.enrichMergeTag, enrichMergeTagInt, null, null);
				AkImporterHelper.print(print, "Done");
			}
		}
		
		// Check if we reimport existing enrichment files or not
		if (!this.reimport) {
			
			if (pathToEnrichFile != null) {
				// Start default enrichment (= no reimport), but only if the enrich file exists and is an XML file
				File enrichFile = new File(pathToEnrichFile);
				boolean enrichFileIsXml = (enrichFile.exists() && enrichFile.isFile() && enrichFile.getName().toLowerCase().endsWith(".xml"));
				if (enrichFileIsXml) {
					AkImporterHelper.print(print, "\nEnriching data from \"" + this.enrichName + "\" in file " + pathToEnrichFile + " to Solr server " + this.enrichSolr + " ... ");
					new Index(pathToEnrichFile, enrichSolrServer, this.enrichProperties, directoryOfTranslationFiles, timeStamp, this.optimize, false);
					AkImporterHelper.print(print, "Done");
				}
			}
		} else {
			// Reimport initial enrichment file based on the enrich.???.localPath.initial property in AkImporter.properties
			AkImporterHelper.print(this.print, "\nReimport enrichment from initial file\n");
			new Index(pathToEnrichFile, enrichSolrServer, this.enrichProperties, directoryOfTranslationFiles, timeStamp, this.optimize, false);
			AkImporterHelper.print(print, "Done");
			
			// Reimport all existing enrichment files based on the enrich.???.localPath.ongoing property in AkImporter.properties
			// Check if a "merged" folder exists. If yes, take the XML files only from there. If not, search for all XML files in the local path.
			File fPathToEnrichDir = new File(AkImporterHelper.stripFileSeperatorFromPath(localPath) + File.separator + "merged");
			if (!fPathToEnrichDir.exists()) {
				fPathToEnrichDir = new File(AkImporterHelper.stripFileSeperatorFromPath(localPath));
			}
			
			// Get a sorted list (oldest to newest) from all ongoing data deliveries:
			List<File> fileList = (List<File>)FileUtils.listFiles(fPathToEnrichDir, new String[] {"xml"}, true); // Get all xml-files recursively
			Collections.sort(fileList); // Sort oldest to newest
			for (File file : fileList) {
				AkImporterHelper.print(this.print, "\nReimport enrichment from ongoing file " + (fileList.indexOf(file)+1) + " of " + fileList.size() + "\n");
				String originalTimestamp = file.getName().replace(".xml", "");
				String originalEnrichmentDate = DateFormatUtils.format(Long.valueOf(originalTimestamp), "dd.MM.yyyy HH:mm:ss");
				AkImporterHelper.print(this.print, "Original enrichment time: " + originalEnrichmentDate + "\n");
				
				// Enrich metadata to Solr
				new Index(file.getAbsolutePath(), enrichSolrServer, this.enrichProperties, directoryOfTranslationFiles, timeStamp, this.optimize, false);
			}
			
		}
	}

}
