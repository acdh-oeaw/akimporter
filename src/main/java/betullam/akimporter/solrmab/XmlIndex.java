package main.java.betullam.akimporter.solrmab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.indexing.XmlContentHandler;
import main.java.betullam.akimporter.updater.ExtractTarGz;
import main.java.betullam.akimporter.updater.FtpDownload;

public class XmlIndex {

	private String xmlName;
	private String path;
	private String propertiesFile;
	private String solrBibl;
	private String elements;
	private List<String> include;
	private List<String> exclude;
	private String deleteBeforeImport;
	private boolean ftpDownload;
	private String xmlFtpHost;
	private int xmlFtpPort;
	private String xmlFtpUser;
	private String xmlFtpPass;
	private String xmlFtpRemotePath;
	//private String xmlFtpLocalPath;
	private boolean compareFiles;
	private boolean xmlUnpack;
	private boolean xmlMerge;
	private boolean print;
	private boolean optimize;
	private String indexTimestamp;

	public XmlIndex(
			String xmlName,
			String path,
			String propertiesFile,
			String solrBibl,
			String elements,
			List<String> include,
			List<String> exclude,
			String deleteBeforeImport,
			boolean ftpDownload,
			String xmlFtpHost,
			int xmlFtpPort,
			String xmlFtpUser,
			String xmlFtpPass,
			String xmlFtpRemotePath,
			//String xmlFtpLocalPath,
			boolean compareFiles,
			boolean xmlUnpack,
			boolean xmlMerge,
			boolean print,
			boolean optimize) {

		this.xmlName = xmlName;
		this.path = path;
		this.propertiesFile = propertiesFile;
		this.solrBibl = solrBibl;
		this.elements = elements;
		this.include = include;
		this.exclude = exclude;
		this.deleteBeforeImport = deleteBeforeImport;
		this.ftpDownload = ftpDownload;
		this.xmlFtpHost = xmlFtpHost;
		this.xmlFtpPort = xmlFtpPort;
		this.xmlFtpUser = xmlFtpUser;
		this.xmlFtpPass = xmlFtpPass;
		this.xmlFtpRemotePath = xmlFtpRemotePath;
		//this.xmlFtpLocalPath = xmlFtpLocalPath;
		this.compareFiles = compareFiles;
		this.xmlUnpack = xmlUnpack;
		this.xmlMerge = xmlMerge;
		this.print = print;
		this.optimize = optimize;
		this.indexTimestamp = String.valueOf(new Date().getTime());

		this.xmlGenericIndexing();
	}


	private boolean xmlGenericIndexing() {
		boolean isIndexingSuccessful = false;
		
		AkImporterHelper.print(print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(print, "\nStarting XML importing: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));

		List<String> filesToUnpack = null;
		
		if (this.ftpDownload) {
			if (this.xmlFtpHost != null && this.xmlFtpUser != null && this.xmlFtpPass != null && this.path != null) {
				String localBasePath = this.path + File.separator + "original";
				FtpDownload ftpDownload = new FtpDownload();
				ftpDownload.downloadFiles(this.xmlFtpRemotePath, null, localBasePath, this.xmlFtpHost, this.xmlFtpPort, this.xmlFtpUser, this.xmlFtpPass, this.indexTimestamp, this.compareFiles, true);
				filesToUnpack = ftpDownload.getDownloadedFiles();
			} else {
				System.err.println("Error: Check if settings \"xml."+this.xmlName+".path\", \"xml."+this.xmlName+".ftpHost\", \"xml."+this.xmlName+".ftpUser\" and \"xml."+this.xmlName+".ftpPass\" are set in AkImporter.properties.");
				System.exit(1);
			}
		} else {
			filesToUnpack = new ArrayList<String>();
			for (File file : FileUtils.listFiles(new File(this.path), null, true)) {
				if (file.isFile()) {
					filesToUnpack.add(file.getAbsolutePath());
				}
				
			}
		}
				
		if (this.xmlUnpack && filesToUnpack	!= null && !filesToUnpack.isEmpty()) {
			AkImporterHelper.print(print, "\nStart extracting files ... ");
			String originalBasePath = (this.ftpDownload) ? this.path + File.separator + "original" : this.path;
			String extractedBasePath = this.path + File.separator + "extracted";
			//List<String> relativeFilePathsToUnpack = getLocalFiles(originalBasePath);
			//System.out.println("\nrelativeFilePathsToUnpack: " + relativeFilePathsToUnpack);
			
			for (String fileToUnpack : filesToUnpack) {
				
				// Get (sub)folder for extracted files and create it
				String localPathExtracted = new File(fileToUnpack.replace(originalBasePath, extractedBasePath)).getParent();
				AkImporterHelper.mkDirIfNotExists(localPathExtracted);
				
				// Extract file
				ExtractTarGz extractor = new ExtractTarGz();
				extractor.extractGeneric(fileToUnpack, this.indexTimestamp, localPathExtracted);
				
			}
			
			/*
			for (String relativeFileToUnpack : relativeFilePathsToUnpack) {
				String relativeFileToUnpackParent = new File(relativeFileToUnpack).getParent();
				File fToUnpack = new File(originalBasePath + File.separator + relativeFileToUnpack);
				if (fToUnpack.isFile()) {
					//String parentFolder = fToUnpack.getParent();
					String directories = (relativeFileToUnpackParent != null && !relativeFileToUnpackParent.isEmpty()) ? File.separator + relativeFileToUnpackParent : "";
					String localPathExtracted = this.path + File.separator + "extracted" + directories;
					AkImporterHelper.mkDirIfNotExists(localPathExtracted);
					ExtractTarGz extractor = new ExtractTarGz();
					extractor.extractGeneric(fToUnpack.getAbsolutePath(), this.indexTimestamp, localPathExtracted);
				}
			}
			*/
			
			AkImporterHelper.print(print, "Done");
		}
		
		
		/*
		// Creating Solr server
		HttpSolrServer sServerBiblio =  new HttpSolrServer(this.solrBibl);

		File xmlFile = new File(this.path);

		List<File> fileList = new ArrayList<File>();
		if (xmlFile.isDirectory()) {
			fileList = (List<File>)FileUtils.listFiles(xmlFile, new String[] {"xml"}, true); // Get all xml-files recursively
		} else {
			fileList.add(xmlFile);
		}

		
		// XML Validation
		boolean allFilesValid = false;
		AkImporterHelper.print(print, "\nStart validating XML data ... ");
		XmlValidator bxh = new XmlValidator();
		for (File file : fileList) {
			boolean hasValidationPassed = bxh.validateXML(file.getAbsolutePath());
			if (hasValidationPassed) {
				allFilesValid = true;
			} else {
				allFilesValid = false;
				System.err.println("Error in file " + file.getName() + ". Import process was cancelled.");
				return allFilesValid;
			}
		}
		// If all files are valid, go on with the import process
		if (allFilesValid) {
			AkImporterHelper.print(print, "Done\n");
		} else {
			// If there are errors in at least one file, stop the import process:
			System.err.println("\nError while validating. Import process was cancelled!\n");
			return false;
		}
		
		if (deleteBeforeImport != null && !deleteBeforeImport.trim().isEmpty()) {
			AkImporterHelper.deleteRecordsByQuery(sServerBiblio, deleteBeforeImport);
		}
		
		for (File file : fileList) {
			isIndexingSuccessful = indexXmlData(file.getAbsolutePath(), sServerBiblio);

			if (isIndexingSuccessful) {
				try {
					// Commit to Solr server:
					sServerBiblio.commit();
					
					if (optimize) {
						AkImporterHelper.print(print, "\nOptimizing Solr Server ... ");
						AkImporterHelper.solrOptimize(sServerBiblio);
						AkImporterHelper.print(print, "Done");
					}
				} catch (SolrServerException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		*/
		
		return isIndexingSuccessful;

	}
	
	
	public boolean indexXmlData(String fileName, HttpSolrServer solrServerBiblio) {
		
		boolean isIndexingSuccessful = false;
		
		try {
			// Create InputSource from XML file
			FileReader xmlData = new FileReader(fileName);
			InputSource inputSource = new InputSource(xmlData);

			// Create variable for content handler
			ContentHandler contentHandler = null;

			// Create content handler for generic XML data
			contentHandler = new XmlContentHandler(solrServerBiblio, elements, include, exclude, propertiesFile, indexTimestamp, print);
			
			// Create SAX parser and set content handler:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();
			
			// Set SAX parser namespace aware (namespaceawareness)
			xmlReader.setFeature("http://xml.org/sax/features/namespace-prefixes", true);
			
			// Set the content handler for the SAX parser
			xmlReader.setContentHandler(contentHandler);

			// Start parsing & indexing:
			AkImporterHelper.print(print, "\nIndexing documents to Solr ... ");
			xmlReader.parse(inputSource);
			isIndexingSuccessful = true;
			if (isIndexingSuccessful) {
				AkImporterHelper.print(print, "Done");
				AkImporterHelper.print(print, "\nEVERYTHING WAS SUCCESSFUL");
			} else {
				AkImporterHelper.print(print, "ERROR");
			}
			
		} catch (FileNotFoundException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (SAXException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (IOException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		}

		return isIndexingSuccessful;
	}
	
	
	private List<String> getFileTreeDiff (String localBasePath, String remoteBasePath) {
		AkImporterHelper.print(this.print, "\nStart comparing file trees and getting the difference ... ");
		
		List<String> filesToDownload = null;
		
		// Get local file names
		List<String> localFileNames = null;
		localFileNames = getLocalFiles(localBasePath);

		// Get remote file names
		List<String> remoteFileNames = null;
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect(this.xmlFtpHost, this.xmlFtpPort);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpClient.login(this.xmlFtpUser, this.xmlFtpPass);
			remoteFileNames = getRemoteFiles(ftpClient, remoteBasePath, "", new ArrayList<String>());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Compare the two lists (local and remote)
		remoteFileNames.removeAll(localFileNames);
		
		// Check if there is a difference between the lists
		if (!remoteFileNames.isEmpty()) {
			filesToDownload = remoteFileNames;
		}
		
		AkImporterHelper.print(this.print, "Done");
		
		return filesToDownload;
	}
	
	
	private List<String> getLocalFiles(String localBasePath) {
		List<String> localFileNames = new ArrayList<String>();
		
		File localBaseFile = new File(localBasePath);
		List<File> localFiles = new ArrayList<File>();
		if (localBaseFile.isDirectory()) {
			localFiles = (List<File>)FileUtils.listFiles(localBaseFile, null, true);
		}
		
		for(File localFile : localFiles) {
			String localRelativeFilePath = new File(localBasePath).toURI().relativize(localFile.toURI()).getPath();
			localFileNames.add(localRelativeFilePath);
		}
		
		return localFileNames;
	}
	
	
	private List<String> getRemoteFiles(FTPClient ftpClient, String remoteBasePath, String remoteRelativePath, List<String> remoteFileNames) {
		FTPFile[] ftpFiles;
		try {
			
			// Get files in current FTP folder
			ftpFiles = ftpClient.listFiles(remoteBasePath + File.separator + remoteRelativePath);
			
			// Iterate over files
			for (FTPFile ftpFile : ftpFiles) {
				// Variable for file name
				String relativeFtpPath = "";
				
				// Check if current FTP file is a directory or a real file
				if (ftpFile.isDirectory()) {
					// If it is a directory, get the name of it
					String currentDirName = ftpFile.getName();
					
					// Create a new relative path that can be added to the base path and check
					// the directory for files by calling this method again.
					relativeFtpPath += (remoteRelativePath.isEmpty()) ? currentDirName : remoteRelativePath + File.separator + currentDirName;
					getRemoteFiles(ftpClient, remoteBasePath, relativeFtpPath, remoteFileNames);
				} else {
					// If it is a file, get it's name and add it to a list
					String fileName = ftpFile.getName();			
					String remoteFileName = (remoteRelativePath.isEmpty()) ? fileName : remoteRelativePath + File.separator + fileName;
					remoteFileNames.add(remoteFileName);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return remoteFileNames;
	}
	
}