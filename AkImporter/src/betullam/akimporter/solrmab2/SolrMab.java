package betullam.akimporter.solrmab2;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer.RemoteSolrException;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import betullam.akimporter.main.Main;



public class SolrMab {


	String mabXMLfile;
	String mabPropertiesFile;
	String solrServerName;
	String validateXmlOnly;
	String validateAllFilesInFolder;
	private List<MatchingObject> listOfMatchingObjs; // Contents from mab.properties file
	public static List<String> multiValuedFields = new ArrayList<String>();
	public static List<Mabfield> customTextFields = new ArrayList<Mabfield>();
	public static HashMap<String, List<String>> translateFields = new HashMap<String, List<String>>();
	private boolean useDefaultMabProperties;
	boolean print = true;
	long startTime;
	long endTime;

	public SolrMab() {};
	public SolrMab(boolean print) {
		this.print = print;
	};

	public boolean startIndexing(String mabXmlFile, String solrServerName, String mabPropertiesFile, String pathToTranslationFiles, boolean useDefaultMabProperties) {

		boolean isIndexingSuccessful = false;
		this.mabXMLfile = mabXmlFile;
		this.solrServerName = solrServerName;
		this.mabPropertiesFile = mabPropertiesFile;
		this.useDefaultMabProperties = useDefaultMabProperties;

		setLogger();

		try {
			BufferedInputStream mabPropertiesInputStream = null;
			// Load .properties file:
			if (useDefaultMabProperties) {
				mabPropertiesInputStream = new BufferedInputStream(Main.class.getResourceAsStream("/betullam/akimporter/resources/mab.properties"));
			} else {
				mabPropertiesInputStream = new BufferedInputStream(new FileInputStream(this.mabPropertiesFile));
			}



			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			//++++++++++++++++++++++++++++++++++ PARSING & INDEXING +++++++++++++++++++++++++++++++++//
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			long startTimeOverall = System.currentTimeMillis();
			startTime = System.currentTimeMillis();

			// Get contents of mab.properties files and put them to MatchingObjects
			listOfMatchingObjs = getMatchingObjects(mabPropertiesInputStream, pathToTranslationFiles);

			// Create SAX parser:
			XMLReader xmlReader = XMLReaderFactory.createXMLReader();

			// Create Solr Server:
			HttpSolrServer solrServer = new HttpSolrServer(solrServerName);


			print("\n###############################################################################\n\n");
			print("Start indexing records");
			print("\n-------------------------------------------\n");

			// Specify XML-file to parse:
			FileReader reader = new FileReader(mabXMLfile);
			InputSource inputSource = new InputSource(reader);

			// Set ContentHandler:
			MarcContentHandler marcContentHandler = new MarcContentHandler(listOfMatchingObjs, solrServer);
			xmlReader.setContentHandler(marcContentHandler);

			// Start parsing & indexing:
			xmlReader.parse(inputSource);

			// Commit records:
			solrServer.commit();


			// Report success:
			print("\nDone indexing! Everything worked fine.\n");


			isIndexingSuccessful = true;

			endTime = System.currentTimeMillis();
			print("Indexing to solr took " + getExecutionTime(startTime, endTime));


			//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			//++++++++++++++++++++++++++++++++++ REMOVE VOLUMES FROM PARENTS +++++++++++++++++++++++++++++++++//
			//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			startTime = System.currentTimeMillis();

			// First remove all MU- and serial-volumes. Later on, we will index all volumes again from scratch. Until now there is no other useful solution.
			RemoveMuVolumes rmv = new RemoveMuVolumes();
			rmv.removeMuVolumes(solrServer);
			System.out.print("\n");
			RemoveSerialVolumes rsv = new RemoveSerialVolumes();
			rsv.removeSerialVolumes(solrServer);


			// Commit removals of volumes:
			solrServer.commit();

			endTime = System.currentTimeMillis();
			print("Removing volumes for fresh re-indexing took " + getExecutionTime(startTime, endTime));


			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			//++++++++++++++++++++++++++++++++++ LINKING VOLUMES TO PARENTS +++++++++++++++++++++++++++++++++//
			//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
			startTime = System.currentTimeMillis();

			// Linking MU and MH records
			MuVolumeToParent muVolumeToParent = new MuVolumeToParent();
			muVolumeToParent.addMuRecords(solrServer);

			endTime = System.currentTimeMillis();
			print("Linking MU records took " + getExecutionTime(startTime, endTime));


			startTime = System.currentTimeMillis();

			// Linking serial volumes
			SerialVolumeToParent serialVolumeToParent = new SerialVolumeToParent();
			serialVolumeToParent.addSerialVolumes(solrServer);

			// Commit linking changes:
			solrServer.commit();

			endTime = System.currentTimeMillis();
			print("Linking serial volumes took " + getExecutionTime(startTime, endTime));


			print("\nDone linking parents and childs!\n");
			

			print("\nStart optimizing Solr index. This could take a while. Please wait ...");
			solrServer.optimize();
			print("Done optimizing Solr index.\n"); 

			
			print("Overall time (indexing + linking): " + getExecutionTime(startTimeOverall, endTime));
			print("Everything is done and worked fine.");

		} catch (RemoteSolrException e) {
			isIndexingSuccessful = false;
			System.out.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.out.println("Solr error! Please check Solr-Setting in general.properties file. Maybe it's not set correctly! E. g. it could be a typo. Check also if the Solr-Server is up and running!");
			System.out.println("\n\nSee also StackTrace:\n");
			e.printStackTrace();
			System.out.println("\n-----------------------------------------------------------------------\n");
		} catch (SAXException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			isIndexingSuccessful = false;
			System.out.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.out.println("File error! Most possible reasons for this error:\n");
			System.out.println("\n1. The \"general.properties\"-file is not in directory you are right now (at the moment you are in " + System.getProperty("user.dir") + "). Please change the command promt to the directory where the \"general.properties\"-file is. Also check if it's named correctly.");
			System.out.println("\n2. The XML-file with the data from Aleph is not at the location that is specified in the general.properties-file (check \"mabXMLfile\" there).");
			System.out.println("\n\nSee also StackTrace:\n");
			e.printStackTrace();
			System.out.println("\n-----------------------------------------------------------------------\n");
		} catch (SolrServerException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (IOException e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		} catch (Exception e) {
			isIndexingSuccessful = false;
			e.printStackTrace();
		}

		return isIndexingSuccessful;
	}


	private String getExecutionTime(long startTime, long endTime) {
		String executionTime = null;

		long timeElapsedMilli =  endTime - startTime;
		int seconds = (int) (timeElapsedMilli / 1000) % 60 ;
		int minutes = (int) ((timeElapsedMilli / (1000*60)) % 60);
		int hours   = (int) ((timeElapsedMilli / (1000*60*60)) % 24);

		executionTime = hours + ":" + minutes + ":" + seconds;
		return executionTime;
	}


	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++ MAB PROPERTIES ++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

	private List<MatchingObject> getMatchingObjects(BufferedInputStream propertiesStream, String pathToTranslationFiles) {

		List<MatchingObject> matchingObjects = new ArrayList<MatchingObject>();

		try {
			Properties mabProperties = new Properties();

			// Load contents of properties-file:
			mabProperties.load(propertiesStream);
			propertiesStream.close();

			// Loop through properties, put them into MatcingObjects and add them to a List<MatchingObject>:
			for(String key : mabProperties.stringPropertyNames()) {
				boolean multiValued = false;
				boolean customText = false;
				boolean translateValue = false;
				String strValues = mabProperties.getProperty(key);
				HashMap<String, String> translateProperties = new HashMap<String, String>();
				HashMap<String, List<String>> mabFieldnames = new HashMap<String, List<String>>();
				List<String> fieldsToRemove = new ArrayList<String>();

				// Create CHANGABLE list:
				List<String> lstValues = new ArrayList<String>();
				lstValues.addAll(Arrays.asList(strValues.split("\\s*,\\s*")));

				// Get all multiValued fields and remove them after we finished:
				if (lstValues.contains("multiValued")) {
					multiValued = true;
					lstValues.remove(lstValues.indexOf("multiValued"));
					for(String lstValue : lstValues) {
						mabFieldnames.put(lstValue, null);
						fieldsToRemove.add(lstValue);
					}
					lstValues.removeAll(fieldsToRemove);
					fieldsToRemove.clear();
				}

				// Get all customText fields and remove them after we finished:
				if (lstValues.contains("customText")) {
					customText = true;
					lstValues.remove(lstValues.indexOf("customText"));
					for(String lstValue : lstValues) {
						mabFieldnames.put(lstValue, null);
						fieldsToRemove.add(lstValue);
					}
					lstValues.removeAll(fieldsToRemove);
					fieldsToRemove.clear();
				}

				// Get all translateValue fields and remove them after we finished:
				if (lstValues.contains("translateValue")) {
					if (lstValues.toString().contains(".properties")) {
						translateValue = true;
						// Remove non-mabfield value from the matching-values list:
						lstValues.remove(lstValues.indexOf("translateValue"));

						// Get the filename with the help of RegEx:
						String filename = "";
						Pattern patternPropFile = java.util.regex.Pattern.compile("[^\\s,;]*\\.properties"); // No (^) whitespaces (\\s), commas or semicolons (,;) before ".properties"-string.
						Matcher matcherPropFile = patternPropFile.matcher("");
						for(String lstValue : lstValues) {
							matcherPropFile.reset(lstValue);
							if(matcherPropFile.find()) {
								filename = matcherPropFile.group();
								fieldsToRemove.add(lstValue);
							}
						}
						lstValues.removeAll(fieldsToRemove);
						fieldsToRemove.clear();

						// Get the mapping values from .properties file:
						translateProperties = getTranslateProperties(filename, pathToTranslationFiles);

						// Get the count of characters that should be matched (e. g. 051[1-3]: get 1 and 3) and add it to a List<String>.
						// Then add everything to a HashMap<String, List<String>>.
						String from = "";
						String to = "";

						Pattern patternFrom = Pattern.compile("(?<=\\[)\\d*");
						Pattern patternTo = Pattern.compile("\\d*(?=\\])");
						Matcher matcherFrom = patternFrom.matcher("");
						Matcher matcherTo = patternTo.matcher("");
						for(String lstValue : lstValues) {

							// Get the numbers of the characters in square brackets (e. g. 051[1-3]: get 1 and 3):
							List<String> fromTo = new ArrayList<String>();
							matcherFrom.reset(lstValue);
							from = (matcherFrom.find()) ? matcherFrom.group() : "";
							matcherTo.reset(lstValue);
							to = (matcherTo.find()) ? matcherTo.group() : "";
							fromTo.add(from);
							fromTo.add(to);

							// Remove the square brackets from the mabfield name so that we have a clear mabfield-name:
							String cleanLstValue = lstValue.replaceAll("\\[.*?\\]", "");

							// Add the values to the Mabfield HashMap:
							mabFieldnames.put(cleanLstValue, fromTo);
							fieldsToRemove.add(lstValue);
						}

						lstValues.removeAll(fieldsToRemove);
						fieldsToRemove.clear();
					} else {
						System.out.println("Error: You need to specify a translation-properties file with the file-ending \".properties\"!");
					}
				}

				// Get all default fields (the other fields were removed):
				for(String lstValue : lstValues) {
					mabFieldnames.put(lstValue, null);
					fieldsToRemove.add(lstValue);
				}
				lstValues.removeAll(fieldsToRemove);
				fieldsToRemove.clear();

				matchingObjects.add(new MatchingObject(key, mabFieldnames, multiValued, customText, translateValue, translateProperties));
			}

		} catch (IOException e) {
			matchingObjects = null;			
			System.out.println("\n------------------------------------------------------------------------------------------------------------\n");
			System.out.println("File error! The mab.properties-file was not found. Please check \"mabPropertiesFile\"-setting in general.properties file. Maybe it's not set correctly! E. g. it could be a typo.");
			System.out.println("\nSee also StackTrace:\n");
			e.printStackTrace();
			System.out.println("\n-----------------------------------------------------------------------\n");
		}

		for (MatchingObject matchingObject : matchingObjects) {
			if (matchingObject.isMultiValued()) {
				multiValuedFields.add(matchingObject.getSolrFieldname());
			}

			if (matchingObject.isCustomText()) {
				String solrFieldName = matchingObject.getSolrFieldname();
				HashMap<String, List<String>> customTexts = matchingObject.getMabFieldnames();

				//System.out.println("Solr Fieldname: " + solrFieldName + " - " + customTexts + " - (Multivalued: " + matchingObject.isMultiValued() + " - Custom text: " + matchingObject.isCustomText() + ")");

				// Make new Mabfield for each custom text and add it to a List of Mabfields, so we can process it later on:
				for (Entry<String, List<String>> customText : customTexts.entrySet()) {
					//System.out.println("1. Solr Fieldname: " + solrFieldName + ": " + customText);
					customTextFields.add(new Mabfield(solrFieldName, customText.getKey()));
				}
			}

			if (matchingObject.isTranslateValue()) {
				translateFields = matchingObject.getMabFieldnames();
			}

		}

		return matchingObjects;
	}



	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++ TRANSLATE PROPERTIES ++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

	private HashMap<String, String> getTranslateProperties(String filename, String pathToTranslationFiles) {

		HashMap<String, String> translateProperties = new HashMap<String, String>();

		Properties properties = new Properties();
		String translationFile = pathToTranslationFiles + File.separator + filename;
		BufferedInputStream translationStream = null;

		try {
			// Get .properties file and load contents:
			if (useDefaultMabProperties) {
				translationStream = new BufferedInputStream(Main.class.getResourceAsStream("/betullam/akimporter/resources/" + filename));
			} else {
				translationStream = new BufferedInputStream(new FileInputStream(translationFile));
			}
			properties.load(translationStream);
			translationStream.close();
		} catch (FileNotFoundException e) {
			System.err.println("Error: File not found! Please check if the file \"" + translationFile + "\" is in the same directory as mab.properties.\n");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Get values from general.properties and assign them to the appropriate variables:
		validateXmlOnly = properties.getProperty("validateXML");

		for (Map.Entry<?, ?> property : properties.entrySet()) {
			String key = (String)property.getKey();
			String value = (String)property.getValue();
			translateProperties.put(key, value);
		}

		return translateProperties;
	}


	private void print(String text) {
		if (print) {
			System.out.println(text);
		}
	}



	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
	//++++++++++++++++++++++++++++++++++ LOG4J ++++++++++++++++++++++++++++++++++//
	//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
	private void setLogger() {
		// Log-Output (avoid error message "log4j - No appenders could be found for logger"):
		BasicConfigurator.configure();
		// Set log4j-output to "warn" (avoid very long logs in console):
		Logger.getRootLogger().setLevel(Level.WARN);
	}
}
