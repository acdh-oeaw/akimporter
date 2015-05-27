package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;


public class MarcContentHandler implements ContentHandler {

	MatchingOperations matchingOps = new MatchingOperations();
	//List<Mabfield> allControlfields;
	//List<Mabfield> allDatafields;
	List<Mabfield> allFields;
	List<Record> allRecords;
	//List<Mabfield> allSubfieldsOfDatafield;
	private String nodeContent;
	private Record record;
	private Mabfield controlfield;
	private Mabfield datafield; // Datafield is conentenated of datafield and subfield
	private List<MatchingObject> listOfMatchingObjs;
	private SolrServer sServer;
	private String recordID;
	private boolean is001;
	private boolean print = true;
	int counter = 0;
	
	String controlfieldTag;
	String datafieldTag;
	String datafieldInd1;
	String datafieldInd2;
	String subfieldCode;

	public MarcContentHandler(List<MatchingObject> listOfMatchingObjs, HttpSolrServer solrServer, boolean print) {
		this.listOfMatchingObjs = listOfMatchingObjs;
		this.sServer = solrServer;
		this.print = print;
	}



	@Override
	public void startDocument() throws SAXException {
		// On document-start, crate new list to hold all parsed AlephMARCXML-records:
		allRecords = new ArrayList<Record>();
	}


	// Read and process parameters of XML-Tags and create objects here.
	// Content (text) is processed in endElement()!
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attribs) throws SAXException {

		// Empty node content (text), else, there will be problems with html-encoded characters (&lt;) at
		// character()-method:
		nodeContent = "";

		// If the parser encounters the start of the "record"-tag, create new List to hold the fields of
		// this record and a new record-object to add these list:
		if(localName.equals("record")) {
			allFields = new ArrayList<Mabfield>();
			record = new Record();
		}


		// If the parser encounters the start of the "controlfield"-tag, create a new mabfield-object, get the XML-attributes, set them on the object and add it to the "record"-object:
		if(localName.equals("controlfield")) {
			controlfieldTag = attribs.getValue("tag");
			controlfield = new Mabfield();
			controlfield.setFieldname(controlfieldTag);
		}

		if(localName.equals("datafield")) {
			//allSubfieldsOfDatafield = new ArrayList<Mabfield>();
			datafieldTag = attribs.getValue("tag").trim();
			datafieldInd1 = attribs.getValue("ind1").trim();
			datafieldInd2 = attribs.getValue("ind2").trim();

			// Set empty tags and indicators to a character, so that the string to match against is always
			// of the same length, e. g. "311$ab$c" and "000$**$*". This prevents errors.
			datafieldTag = (datafieldTag != null && !datafieldTag.isEmpty()) ? datafieldTag : "000";
			datafieldInd1 = (datafieldInd1 != null && !datafieldInd1.isEmpty()) ? datafieldInd1 : "*";
			datafieldInd2 = (datafieldInd2 != null && !datafieldInd2.isEmpty()) ? datafieldInd2 : "*";

			is001 = (datafieldTag.equals("001")) ? true : false;
		}

		if(localName.equals("subfield")) {
			subfieldCode = attribs.getValue("code").trim();
			subfieldCode = (subfieldCode != null && !subfieldCode.isEmpty()) ? subfieldCode : "-";

			String datafieldName = datafieldTag + "$" + datafieldInd1 + datafieldInd2 + "$" + subfieldCode;

			// Create a new mabfield so that we can concentenate a datafield and a subfield to a mabfield
			// E. g.: Datafield = 100$**, Subfield = r, Subfield content = AC123456789
			//        Result: mabfield name = 100$**$r, mabfield value = AC123456789
			datafield = new Mabfield();
			datafield.setFieldname(datafieldName);
		}


	}


	// Process content (text) of current XML-node (not working in startElement()) - see also characters():
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {

		counter = counter + 1;

		if(localName.equals("controlfield") ) {
			String controlfieldText = nodeContent.toString();
			controlfield.setFieldvalue(controlfieldText);
			allFields.add(controlfield);
		}


		if(localName.equals("subfield")) {
			String subfieldText = nodeContent.toString();
			datafield.setFieldvalue(subfieldText);
			allFields.add(datafield);

			if (is001 == true) {
				recordID = subfieldText;
			}
		}

		if(localName.equals("datafield")) {
			// Do nothing
		}

		// If the parser encounters the end of the "record"-tag, add all
		// leader-, controlfield- and datafield-objects to the record-object and add the
		// record-object to the list of all records:
		if(localName.equals("record")) {
			record.setMabfields(allFields);
			record.setRecordID(recordID);
			allRecords.add(record);

			/** Every n-th record, match the Mab-Fields to the Solr-Fields, write an appropirate object, loop through the object and
			 * index it's values to Solr, then empty all objects (set to "null") to save memory and go on with the next n records.
			 * If there is a rest, do it in the endDocument()-Method. E. g. modulo is set to 100 and we have 733 records, but now
			 * only 700 are indexed! The 33 remaining records will be indexed in endDocument()-Method;
			 */

			if (counter % 300 == 0) {

				// Do the Matching and rewriting (see class "MatchingOperations"):
				List<Record> newRecordSet = matchingOps.matching(allRecords, listOfMatchingObjs);

				// Add to Solr-Index:
				this.solrAddRecordSet(sServer, newRecordSet);
				//long opStartTime = System.nanoTime();
				//System.out.println(getDuration(opStartTime));

				// Set all Objects to "null" to save memory
				// TODO: Are the followoing really all objects which should be set to "null"?
				allRecords = null;
				allRecords = new ArrayList<Record>();
				newRecordSet = null;
			}
		}



	}


	@Override
	public void endDocument() throws SAXException {

		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		//+++++++++++++++ Add the remaining rest of the records to the index (see modulo-operation with "%"-operator in endElement()) +++++++++++++++//
		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

		// Do the Matching and rewriting (see class "MatchingOperations"):
		List<Record> newRecordSet = matchingOps.matching(allRecords, listOfMatchingObjs);

		// Add to Solr-Index:
		this.solrAddRecordSet(sServer, newRecordSet);

		// Clear Objects to save memory
		// TODO: Are the followoing really all objects which should be set to "null"?
		allRecords.clear();
		newRecordSet.clear();	

	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		// Reads the content of the current XML-node:
		nodeContent += new String(ch, start, length);
	}


	public void solrAddRecordSet(SolrServer sServer, List<Record> recordSet) {
		try {

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			Collection<SolrInputDocument> mhAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

			for (Record record : recordSet) {

				// Create a document:
				SolrInputDocument doc = new SolrInputDocument();

				for (Mabfield mf : record.getMabfields()) {

					String fieldName = mf.getFieldname();
					String fieldValue = mf.getFieldvalue();

					// Add the fieldname and fieldvalue to the document:
					doc.addField(fieldName, fieldValue);

				}

				// Add the document to the collection of documents:
				docs.add(doc);

				print("Indexing record with ID: " + doc.getFieldValue("id"));

			}

			if (docs.isEmpty() == false) {
				// Now add the collection of documents to Solr:
				sServer.add(docs);

				// Set "docs" to "null" (save memory):
				docs = null;
			}

			if (mhAtomicUpdateDocs.isEmpty() == false) {
				// Now add the collection of documents to Solr:
				sServer.add(mhAtomicUpdateDocs);

				// Set "mhAtomicUpdateDocs" to "null" (save memory):
				mhAtomicUpdateDocs = null;
			}

		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	public List<Record> getRecordSet() {
		return this.allRecords;
	}

	public void solrClearIndex(SolrServer sServer) {

		try {
			// Delete the whole Solr-index by query!
			sServer.deleteByQuery( "*:*" );
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void solrCommit(SolrServer sServer) {		

		try {
			// Do a Solr commit 
			sServer.commit();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void printToConsole(List<Record> allRecords) {
		// Print the RecordSet to the console:
		if (print) {
			for (Record record : allRecords) {
				System.out.println("\n----------------------------------------------------------------\n");
				for (Mabfield mf : record.getMabfields()) {
					System.out.println("\t" + mf.getFieldname()+ ":\t" + mf.getFieldvalue());
				}
			}
		}
	}


	// Get time that an operation takes
	public String getDuration(long operationStartTime) {		
		long operationEndTime = System.nanoTime();
		long nanoseconds = operationEndTime - operationStartTime;
		//Double hours = Math.floor(TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS) / 3600);
		//Double minutes = Math.floor((TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS) / 60) % 60);
		Long miliseconds = TimeUnit.MILLISECONDS.convert(nanoseconds, TimeUnit.NANOSECONDS) % 60;
		Long seconds = TimeUnit.SECONDS.convert(nanoseconds, TimeUnit.NANOSECONDS) % 60;

		//System.out.println("OP took " + seconds + " seconds / " + miliseconds +" miliseconds");
		return seconds + " s / " + miliseconds + " ms / " + nanoseconds + " ns";
	}



	@Override
	public void endPrefixMapping(String arg0) throws SAXException {}

	@Override
	public void ignorableWhitespace(char[] arg0, int arg1, int arg2) throws SAXException {}

	@Override
	public void processingInstruction(String arg0, String arg1) throws SAXException {}

	@Override
	public void setDocumentLocator(Locator arg0) {}

	@Override
	public void skippedEntity(String arg0) throws SAXException {}

	@Override
	public void startPrefixMapping(String arg0, String arg1) throws SAXException {}


	private void print(String text) {
		if (print) {
			System.out.println(text);
		}
	}

}
