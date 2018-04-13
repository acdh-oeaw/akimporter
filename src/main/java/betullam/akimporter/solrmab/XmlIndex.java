package main.java.betullam.akimporter.solrmab;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import ak.xmlhelper.XmlMerger;
import ak.xmlhelper.XmlValidator;
import main.java.betullam.akimporter.converter.Converter;
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
	private boolean compareFiles;
	private boolean xmlUnpack;
	private boolean convertMarcBin2MarcXml;
	private boolean xmlMerge;
	private String xmlMergeTag;
	private String xmlMergeLevel;
	private String xmlMergeParentTag;
	private String xmlMergeParentAttributes;
	private String mergeElementAttributes;
	private boolean xmlIsMarcXml;
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
			boolean compareFiles,
			boolean xmlUnpack,
			boolean convertMarcBin2MarcXml,
			boolean xmlMerge,
			String xmlMergeTag,
			String xmlMergeLevel,
			String xmlMergeParentTag,
			String xmlMergeParentAttributes,
			String mergeElementAttributes,
			boolean xmlIsMarcXml,
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
		this.compareFiles = compareFiles;
		this.xmlUnpack = xmlUnpack;
		this.convertMarcBin2MarcXml = convertMarcBin2MarcXml;
		this.xmlMerge = xmlMerge;
		this.xmlMergeTag = xmlMergeTag;
		this.xmlMergeLevel = xmlMergeLevel;
		this.xmlMergeParentTag = xmlMergeParentTag;
		this.xmlMergeParentAttributes = xmlMergeParentAttributes;
		this.mergeElementAttributes = xmlMergeParentAttributes;
		this.xmlIsMarcXml = xmlIsMarcXml;
		this.print = print;
		this.optimize = optimize;
		this.indexTimestamp = String.valueOf(new Date().getTime());

		this.xmlGenericIndexing();
	}


	private boolean xmlGenericIndexing() {
		boolean isIndexingSuccessful = false;
		
		AkImporterHelper.print(this.print, "\n-----------------------------------------------------------------------------");
		AkImporterHelper.print(this.print, "\nStarting XML importing: " + new SimpleDateFormat("dd.MM.yyyy HH:mm:ss").format(new Date(Long.valueOf(this.indexTimestamp))));

		List<String> downloadedFiles = null;
		List<String> extractedFiles = null;
		List<String> mergedFiles = new ArrayList<String>();
		
		if (this.ftpDownload) {
			if (this.xmlFtpHost != null && this.xmlFtpUser != null && this.xmlFtpPass != null && this.path != null) {
				String localBasePath = this.path + File.separator + "original";
				FtpDownload ftpDownload = new FtpDownload();
				ftpDownload.downloadFiles(this.xmlFtpRemotePath, null, localBasePath, this.xmlFtpHost, this.xmlFtpPort, this.xmlFtpUser, this.xmlFtpPass, this.indexTimestamp, this.compareFiles, true);
				downloadedFiles = ftpDownload.getDownloadedFiles();
			} else {
				System.err.println("\nError: Check if settings \"xml."+this.xmlName+".path\", \"xml."+this.xmlName+".ftpHost\", \"xml."+this.xmlName+".ftpUser\" and \"xml."+this.xmlName+".ftpPass\" are set in AkImporter.properties.");
				System.exit(1);
			}
		} else {
			downloadedFiles = new ArrayList<String>();
			for (File file : FileUtils.listFiles(new File(this.path), null, true)) {
				if (file.isFile()) {
					downloadedFiles.add(file.getAbsolutePath());
				}
			}
		}
		
		if (this.xmlUnpack && downloadedFiles != null && !downloadedFiles.isEmpty()) {
			AkImporterHelper.print(this.print, "\nStart extracting files ... ");
			String originalBasePath = (this.ftpDownload) ? this.path + File.separator + "original" : this.path;
			String extractedBasePath = this.path + File.separator + "extracted";
			ExtractTarGz extractor = new ExtractTarGz();
			
			for (String fileToUnpack : downloadedFiles) {
				// Set destination (sub)folder(s) for extracted files
				String localPathExtracted = new File(fileToUnpack.replace(originalBasePath, extractedBasePath)).getParent();
				
				// Extract file (destination [sub]folder[s] will be created if they don't exist)
				extractor.extractGeneric(fileToUnpack, this.indexTimestamp, localPathExtracted);
			}
			extractedFiles = extractor.getExtractedFiles();
			AkImporterHelper.print(this.print, "Done");
		} else {
			extractedFiles = downloadedFiles;
		}

		if (this.convertMarcBin2MarcXml && extractedFiles != null && !extractedFiles.isEmpty()) {
			AkImporterHelper.print(this.print, "\nConverting files ... ");
			Converter converter = new Converter();
			String convertedBasePath = this.path + File.separator + "converted";
			AkImporterHelper.mkDirIfNotExists(convertedBasePath);
			
			for(String fileToConvert : extractedFiles) {
				if (fileToConvert.endsWith(".mrc")) { // Check for binary MARC file ending as only MARC files can be indexed.
					converter.marcBin2MarcXml(fileToConvert, convertedBasePath);
				}
			}
			
			extractedFiles = converter.getConvertedFiles();
			AkImporterHelper.print(this.print, "Done");
		}

		if ((this.xmlMerge || this.convertMarcBin2MarcXml) && extractedFiles != null && !extractedFiles.isEmpty()) {
			AkImporterHelper.print(this.print, "\nMerging files ... ");
			
			String sourceBasePath = this.path;
			if (this.ftpDownload) {
				sourceBasePath = this.path + File.separator + "original";
			}
			if (this.xmlUnpack) {
				sourceBasePath = this.path + File.separator + "extracted";
			}
			if (this.convertMarcBin2MarcXml) {
				sourceBasePath = this.path + File.separator + "converted";
			}
			String mergedBasePath = this.path + File.separator + "merged";
			
			Set<String> pathsToFilesToMerge = new HashSet<String>();
			for(String fileToMerge : extractedFiles) {
				File fToMerge = new File(fileToMerge);
				if (fToMerge.isFile()) {
					pathsToFilesToMerge.add(fToMerge.getParent());
				}
			}
						
			if (pathsToFilesToMerge != null && !pathsToFilesToMerge.isEmpty()) {
				for(String pathToFilesToMerge : pathsToFilesToMerge) {
					String localPathMerged = pathToFilesToMerge.replace(sourceBasePath, mergedBasePath);
					AkImporterHelper.mkDirIfNotExists(localPathMerged);
					String pathToMergedFile = localPathMerged + File.separator + this.indexTimestamp + ".xml";
					
					XmlMerger xmlm = new XmlMerger();
					int xmlMergeLevelInt = Integer.valueOf(this.xmlMergeLevel);
					boolean mergeSuccess = xmlm.mergeElements(pathToFilesToMerge, pathToMergedFile, this.xmlMergeParentTag, this.xmlMergeTag, xmlMergeLevelInt, this.xmlMergeParentAttributes, this.mergeElementAttributes);
					if (mergeSuccess) {
						mergedFiles.add(pathToMergedFile);
					}
				}
			}
			AkImporterHelper.print(this.print, "Done");
		} else {
			// Base files.
			for (String downloadedFile : downloadedFiles) {
				if (downloadedFile.endsWith(".xml")) { // Check for XML as only XML files can be indexed.
					mergedFiles.add(downloadedFile);
				}
			}
			
			// If files were extracted, use these.
			if (this.xmlUnpack) {
				mergedFiles.clear(); // Clear the list
				for (String extractedFile : extractedFiles) {
					if (extractedFile.endsWith(".xml")) { // Check for XML as only XML files can be indexed.
						mergedFiles.add(extractedFile);
					}
				}
			}
		}
				
		if (mergedFiles != null && !mergedFiles.isEmpty()) {
			// Creating Solr server
			HttpSolrServer sServerBiblio =  new HttpSolrServer(this.solrBibl);
			
			// Sorting for correct indexing (oldest first)
			Collections.sort(mergedFiles);
			
			for (String fileToImport : mergedFiles) {
				File xmlFile = new File(fileToImport);
				List<File> fileList = new ArrayList<File>();
				if (xmlFile.isDirectory()) {
					fileList = (List<File>)FileUtils.listFiles(xmlFile, new String[] {"xml"}, true); // Get all xml-files recursively
				} else {
					if (FilenameUtils.getExtension(xmlFile.getAbsolutePath()).equals("xml")) {
						fileList.add(xmlFile);
					}
				}

				if (fileList != null && !fileList.isEmpty()) {
					// XML Validation
					boolean allFilesValid = false;
					AkImporterHelper.print(this.print, "\nStart validating XML data ... ");
					XmlValidator bxh = new XmlValidator();
					for (File file : fileList) {
						boolean hasValidationPassed = bxh.validateXML(file.getAbsolutePath());
						if (hasValidationPassed) {
							allFilesValid = true;
						} else {
							allFilesValid = false;
							System.err.println("\nError in file " + file.getName() + ". Import process was cancelled.");
							return allFilesValid;
						}
					}
					
					// If all files are valid, go on with the import process
					if (allFilesValid) {
						AkImporterHelper.print(this.print, "Done");
					} else {
						// If there are errors in at least one file, stop the import process:
						System.err.println("\nError while validating. Import process was cancelled!\n");
						return false;
					}
					
					if (this.deleteBeforeImport != null && !this.deleteBeforeImport.trim().isEmpty()) {
						AkImporterHelper.deleteRecordsByQuery(sServerBiblio, this.deleteBeforeImport);
					}
					
					for (File file : fileList) {
						if (this.xmlIsMarcXml) {
							// Start indexing by using default properties notation in .properties file
							String directoryOfTranslationFiles = new File(this.propertiesFile).getParent();
							Index index = new Index(
									file.getAbsolutePath(),
									sServerBiblio,
									false,
									this.propertiesFile,
									directoryOfTranslationFiles,
									this.indexTimestamp,
									this.optimize,
									this.print);
							
							isIndexingSuccessful = index.isIndexingSuccessful();
						} else {
							// Start indexing by using xPath notation in .properties file
							isIndexingSuccessful = indexXmlData(file.getAbsolutePath(), sServerBiblio);
						}

						if (isIndexingSuccessful) {
							try {
								// Commit to Solr server:
								sServerBiblio.commit();
								
								if (this.optimize) {
									AkImporterHelper.print(this.print, "\nOptimizing Solr Server ... ");
									AkImporterHelper.solrOptimize(sServerBiblio);
									AkImporterHelper.print(this.print, "Done");
								}
							} catch (SolrServerException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			}
		} else {
			AkImporterHelper.print(this.print, "\nNo file available, nothing was imported.");
		}

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
	
}