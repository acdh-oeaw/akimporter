package betullam.akimporter.main;

import java.io.File;
import java.util.Date;
import java.util.Scanner;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.akimporter.solrmab.Index;
import betullam.akimporter.solrmab.Relate;
import betullam.akimporter.solrmab.SolrMabHelper;
import betullam.xmlhelper.XmlCleaner;
import betullam.xmlhelper.XmlMerger;
import betullam.xmlhelper.XmlValidator;

public class Import {

	HttpSolrServer solrServer = null;
	Scanner scanner = null;
	String timeStamp = null;
	boolean optimize = false;
	boolean print = true;
	String typeOfDataset = null;
	String pathToMabXmlFile = null;
	String pathToMultipleXmlFolder = null;
	String isMergeOk = null;
	String pathToMergedFile = null;
	String isValidationOk = null;
	String isXmlCleanOk = null;
	String solrServerAddress = null;
	String useDefaultMabPropertiesFile = null;
	String pathToMabPropertiesFile = null;
	String isIndexingOk = null;
	String directoryOfTranslationFiles = null;
	String propertiesFileInfo = null;
	private SolrMabHelper smHelper = new SolrMabHelper();

	boolean isMergingSuccessful = false;
	boolean hasValidationPassed = false;
	boolean useDefaultMabProperties = true;
	boolean areTranslationFilesOk = false;
	boolean isIndexingSuccessful = false;
	boolean isRelateSuccessful = false;
	boolean isWithCliArgs = false;


	// With interactive user input
	public Import(boolean optimize, boolean print) {	
		this.optimize = optimize;
		this.print = print;
		this.scanner = new Scanner(System.in);
		this.startImporting();
	}

	// With CLI Args
	public Import(String typeOfDataset, String pathToMabXmlFile, String pathToMultipleXmlFolder, boolean validate, String solrUrl, boolean ownMabProps, String pathToOwnMabProps, boolean optimize, boolean print) {
		isMergeOk = "J";
		isXmlCleanOk = "J";
		isIndexingOk = "J";

		this.isWithCliArgs = true;
		this.typeOfDataset = typeOfDataset;
		this.pathToMabXmlFile = pathToMabXmlFile;
		this.pathToMultipleXmlFolder = pathToMultipleXmlFolder;
		this.isValidationOk = (validate) ? "J" : "U";
		this.solrServerAddress = solrUrl;	
		this.useDefaultMabPropertiesFile = (ownMabProps) ? "N" : "J";
		this.pathToMabPropertiesFile = (ownMabProps) ? pathToOwnMabProps : null;
		this.optimize = optimize;
		this.print = print;
		this.startImporting();

	}

	private void startImporting() {

		if (this.timeStamp == null) {
			this.timeStamp = String.valueOf(new Date().getTime());
		}



		if (!isWithCliArgs) {
			typeOfDataset = Main.getUserInput("\nWie liegt ihr Datenbestand vor?\n 1 = eine große XML-Datei\n 2 = viele einzelne XML-Dateien)?", "1, 2", scanner);
		}

		if (typeOfDataset.equals("1")) { // We have one big XML file
			if (!isWithCliArgs) {
				pathToMabXmlFile = Main.getUserInput("\nWie lautet der Pfad zur XML-Datei?\n Beispiel: /home/username/dateiname.xml)?", "fileExists", scanner);
			}

		} else if (typeOfDataset.equals("2")) { // We have multiple smaller XML files - we need to merge them!
			if (!isWithCliArgs) {
				pathToMultipleXmlFolder = Main.getUserInput("\nWie lautet der Pfad zum Ordner mit den einzelnen XML-Dateien?\n Beispiel: /home/username/xmldateien)?", "directoryExists", scanner);
			}
			
			String tempDir = System.getProperty("java.io.tmpdir");
			tempDir = (tempDir.endsWith("/") || tempDir.endsWith("\\")) ? tempDir : tempDir + System.getProperty("file.separator");
			pathToMergedFile = tempDir + "ImporterMergedFile.xml";

			if (!isWithCliArgs) {
				isMergeOk = Main.getUserInput("\nDie XML-Dateien müssen nun in eine einzige XML-Datei zusammengeführt werden."
						+ " Sie wird im temporären Verzeichnis des Systems unter " + pathToMergedFile + " gespeichert. Die Original-Daten"
						+ " werden nicht geändert. Wollen Sie fortfahren? Falls nicht, wird der gesamte Import-Vorgang abgebrochen!"
						+ "\n J = Ja, fortfahren\n N = Nein, abbrechen", "J, N", scanner);
			}

			if (isMergeOk.equals("J")) {

				/*
				if (!isWithCliArgs) {
					pathToMergedFile = Main.getUserInput("\nGeben Sie an, wo die Datei mit den zusammengeführten Daten gespeichert werden"
							+ " sollen. Geben Sie dazu einen Pfad inkl. Dateiname und der Endung \".xml\" an,"
							+ " z. B.: /home/benutzer/meinedatei.xml. Beachten Sie, dass Sie am angegebenen Ort Schreibberechigungen"
							+ " haben müssen und es NICHT der gleiche Ort sein darf, in dem die einzelnen XML-Dateien liegen.", "newFile", scanner);
				}
				 */

				// Start XML merging:
				XmlMerger xmlm = new XmlMerger();
				isMergingSuccessful = xmlm.mergeElementNodes(pathToMultipleXmlFolder, pathToMergedFile, "collection", "record", 1);

				if (isMergingSuccessful) {
					pathToMabXmlFile = pathToMergedFile;
					this.smHelper.print(this.print, "\nDatenzusammenführung in Datei " + pathToMergedFile + " erfolgreich abgeschlossen.\n");
				} else {
					System.err.println("\nFehler bei der Datenzusammenführung! Vorgang abgebrochen.\n");
					return;
				}
			} else {
				this.smHelper.print(this.print, "\nImport-Vorgang auf Benutzerwunsch abgebrochen.\n");
				return;
			}
		}


		if (!isWithCliArgs) {
			isValidationOk = Main.getUserInput("\nDie XML-Datei muss geprüft werden. Dies kann eine Weile dauern. Die Original-Daten werden nicht geändert. "
					+ "Wollen Sie fortfahren? Falls nicht, wird der gesamte Vorgang abgebrochen! "
					+ "\n J = Ja, fortfahren\n U = Überspringen\n N = Nein, abbrechen", "J, U, N", scanner);
		}

		if (isValidationOk.equals("J") || isValidationOk.equals("U")) {

			if (isValidationOk.equals("J")) {
				this.smHelper.print(this.print, "\nStarte Validierung. Bitte um etwas Geduld ...");
				XmlValidator bxh = new XmlValidator();
				hasValidationPassed = bxh.validateXML(pathToMabXmlFile);

				while (hasValidationPassed == false) {
					this.smHelper.print(this.print, "\nProblem in der XML Datei gefunden!");
					if (!isWithCliArgs) {
						isXmlCleanOk = Main.getUserInput("\nWollen Sie eine Datenbereinigung durchführen? "
								+ "Die Originaldaten werden nicht verändert. "
								+ "Dieser Vorgang kann je nach Datenmenge länger dauern. "
								+ "Wenn Sie keine Datenbereinigung durchführen, wird der Vorgang abgebrochen."
								+ "\n J = Ja, Datenbereinigung durchführen\n N = Nein, Import-Vorgang abbrechen", "J, N", scanner);
					}
					if (isXmlCleanOk.equals("J")) {
						// Start cleaning XML
						XmlCleaner xmlc = new XmlCleaner();
						boolean cleaningProcessDone = xmlc.cleanXml(pathToMabXmlFile);
						boolean isNewXmlFileClean = false;
						if (cleaningProcessDone == true) {
							pathToMabXmlFile = xmlc.getCleanedFile();
							isNewXmlFileClean = bxh.validateXML(xmlc.getCleanedFile());
							if (isNewXmlFileClean == false) {
								this.smHelper.print(this.print, "\nDaten konnten nicht bereinigt werden! Import-Vorgang wurde abgebrochen.");
								return;
							} else {
								hasValidationPassed = true;
							}
						} else {
							this.smHelper.print(this.print, "\nProblem bei der Datenbereinigung! Möglicherweise haben Sie keine"
									+ " Schreibberechtigung für den Ordner, in den die bereinigte Datei geschrieben wird"
									+ " (der gleiche wie die Ausgangsdatei \"" + pathToMabXmlFile + "\").");
							return;
						}
					} else {
						this.smHelper.print(this.print, "\nImport-Vorgang auf Benutzerwunsch abgebrochen!");
						return;
					}
				}
			} else {

				hasValidationPassed = true;
			}


			if (hasValidationPassed) {
				if (isValidationOk.equals("J")) {
					this.smHelper.print(this.print, "\nValidierung war erfolgreich. Die Daten sind nun bereit für die Indexierung.\n");
				}
				if (isValidationOk.equals("U")) {
					this.smHelper.print(this.print, "\nValidierung übersprungen!");
				}

				if (!isWithCliArgs) {
					solrServerAddress = Main.getUserInput("\nGeben Sie die Solr-Serveradresse (URL) inkl. Core-Name ein (z. B. http://localhost:8080/solr/corename)", "solrPing", scanner);
				}
				if (!isWithCliArgs) {
					useDefaultMabPropertiesFile = Main.getUserInput("\nWollen Sie die \"mab.properties\" Datei in der Standardkonfiguration verwenden? "
							+ "Wenn Sie dies nicht wollen, können Sie anschließend einen Pfad zu einer eigenen .properties-Datei angeben."
							+ "\n J = Ja, Standard verwenden\n N = Nein, Standard nicht verwenden", "J, N", scanner);
				}
				if (useDefaultMabPropertiesFile.equals("J")) {
					useDefaultMabProperties = true;
					pathToMabPropertiesFile = Main.class.getResource("/betullam/akimporter/resources/mab.properties").getFile();
					directoryOfTranslationFiles = Main.class.getResource("/betullam/akimporter/resources").getPath();
					propertiesFileInfo = "Standard mab.properties Datei verwenden";
				} else {
					useDefaultMabProperties = false;
					if (!isWithCliArgs) {
						pathToMabPropertiesFile = Main.getUserInput("\nBitte geben den Pfad zu Ihrer eigenen .properties-Datei an (z. B. /home/username/meine.properties). Beachten Sie, dass die Dateiendung wirklich \".properties\" sein muss!", "propertiesExists", scanner);
					}
					propertiesFileInfo = "Eigene .properties Datei verwenden: " + pathToMabPropertiesFile;

					directoryOfTranslationFiles = new File(pathToMabPropertiesFile).getParent();
					areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile, directoryOfTranslationFiles);

					// It the translation files, that are defined in the custom MAB properties file, do not exist
					// (they have to be in the same directory), that give an appropriate message:
					while (areTranslationFilesOk == false) {
						scanner.nextLine();
						areTranslationFilesOk = Main.translationFilesExist(pathToMabPropertiesFile ,directoryOfTranslationFiles);
					}


				}

				if (!isWithCliArgs) {
					isIndexingOk = Main.getUserInput("\nAlles ist nun bereit. Hier noch einmal Ihre Angaben:"
							+ "\n Daten-Datei:\t" + pathToMabXmlFile
							+ "\n Solr Server:\t" + solrServerAddress
							+ "\n .properties:\t" + propertiesFileInfo
							+ "\n\nWollen Sie den Import-Vorgang nun beginnen?"
							+ "\nACHTUNG: Ja nach Datenmenge und Leistung des Computers kann dieser Vorgang lange dauern!"
							+ " \n J = Ja, Import-Vorgang beginnen\n N = Nein, Import-Vorgang abbrechen", "J, N", scanner);
				}

				if (isIndexingOk.equals("J")) {

					if (!isWithCliArgs) {
						this.optimize = true;
					}

					// Create SolrSever:
					solrServer = new HttpSolrServer(solrServerAddress);

					// Index metadata so Solr
					Index index = new Index(pathToMabXmlFile, this.solrServer, useDefaultMabProperties, pathToMabPropertiesFile, directoryOfTranslationFiles, this.timeStamp, false, this.print);
					isIndexingSuccessful = index.isIndexingSuccessful();

					// Connect child and parent volumes:
					Relate relate = new Relate(this.solrServer, this.timeStamp, false, this.print);
					isRelateSuccessful = relate.isRelateSuccessful();
					
					if (this.optimize) {
						this.smHelper = new SolrMabHelper(solrServer);
						this.smHelper.print(this.print, "\nOptimiere Solr Server. Dies kann eine Weile dauern ...\n");
						this.smHelper.solrOptimize();
					}

					if (isIndexingSuccessful && isRelateSuccessful) {
						this.smHelper.print(this.print, "\nImport-Vorgang erfolgreich abgeschlossen.\n");
					} else {
						System.err.println("\nFehler beim Import-Vorgang!\n");
						return;
					}
				} else {
					this.smHelper.print(this.print, "\nImport-Vorgang auf Benutzerwunsch abgebrochen.\n");
					return;
				}

			}
		} else {
			this.smHelper.print(this.print, "\nImport-Vorgang auf Benutzerwunsch abgebrochen!");
			return;
		}

		if (!isWithCliArgs) {
			scanner.close();
		}
	}

}
