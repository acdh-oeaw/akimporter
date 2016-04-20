/**
 * Handles the contents of the MarcXML files.
 * This is where some of the data processing is done to
 * get the values in shape before indexing them to Solr.
 *
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
 * 
 * This file is part of AkImporter.
 * 
 * AkImporter is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AkImporter is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AkImporter.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Michael Birkner <michael.birkner@akwien.at>
 * @license  http://www.gnu.org/licenses/gpl-3.0.html
 * @link     http://wien.arbeiterkammer.at/service/bibliothek/
 */
package main.java.betullam.akimporter.solrmab.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;


public class MarcContentHandler implements ContentHandler {

	MatchingOperations matchingOps = new MatchingOperations();
	List<Mabfield> allFields;
	List<Record> allRecords;
	private String nodeContent;
	private Record record;
	private Mabfield controlfield;
	private Mabfield datafield; // Datafield is conentenated of datafield and subfield
	private List<MatchingObject> listOfMatchingObjs;
	private SolrServer sServer;
	private String recordID;
	private String recordSYS;
	private boolean is001Controlfield;
	private boolean is001Datafield;
	private boolean isSYS;
	private boolean print = true;
	int counter = 0;
	long startTime;
	long endTime;
	String timeStamp;
	int NO_OF_DOCS = 500;

	String controlfieldTag;
	String datafieldTag;
	String datafieldInd1;
	String datafieldInd2;
	String subfieldCode;

	/**
	 * Constructor of MarcContentHandler.
	 * This is the starting point of reading and processing the XML file(s) containing MARC records.
	 * 
	 * @param listOfMatchingObjs	List<MatchingObject>. A MatchingObject contains information about matching MAB fields to Solr fields.
	 * @param solrServer			SolrServer object that represents the Solr server to which the data should be indexed 
	 * @param timeStamp				String that specifies the starting time of the importing process
	 * @param print					boolean. True if status messages should be printed to the console.
	 */
	public MarcContentHandler(List<MatchingObject> listOfMatchingObjs, SolrServer solrServer, String timeStamp, boolean print) {
		this.listOfMatchingObjs = listOfMatchingObjs;
		this.sServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;
	}

	
	
	/**
	 * Executed when encountering the start element of the XML file.
	 */
	@Override
	public void startDocument() throws SAXException {

		// For tracking the elapsed time:
		startTime = System.currentTimeMillis();
		
		// On document-start, crate new list to hold all parsed AlephMARCXML-records:
		allRecords = new ArrayList<Record>();
	}

	
	
	/**
	 * Executed when encountering the start element of an XML tag.
	 * 
	 * Reading and processing XML attributes is done here.
	 * Reading of element content (text) is processed in endElement().
	 */
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
			isSYS = (controlfieldTag.equals("SYS")) ? true : false;
			controlfield = new Mabfield();
			controlfield.setFieldname(controlfieldTag);
			is001Controlfield = (controlfieldTag.equals("001")) ? true : false;
		}

		if(localName.equals("datafield")) {
			datafieldTag = attribs.getValue("tag").trim();
			datafieldInd1 = attribs.getValue("ind1").trim();
			datafieldInd2 = attribs.getValue("ind2").trim();

			// Set empty tags and indicators to a character, so that the string to match against is always
			// of the same length, e. g. "311$ab$c" and "000$**$*". This prevents errors.
			datafieldTag = (datafieldTag != null && !datafieldTag.isEmpty()) ? datafieldTag : "000";
			datafieldInd1 = (datafieldInd1 != null && !datafieldInd1.isEmpty()) ? datafieldInd1 : "*";
			datafieldInd2 = (datafieldInd2 != null && !datafieldInd2.isEmpty()) ? datafieldInd2 : "*";
			is001Datafield = (datafieldTag.equals("001")) ? true : false;
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


	
	/**
	 * Executed when encountering the end element of an XML tag.
	 * 
	 * Reading and processing XML attributes is done here.
	 * Reading of element content (text) is done here (see also characters() method).
	 */
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {


		if(localName.equals("controlfield") ) {
			String controlfieldText = nodeContent.toString();
			controlfield.setFieldvalue(controlfieldText);
			allFields.add(controlfield);
			
			if (isSYS == true) {
				recordSYS = controlfieldText;
			}
			if (is001Controlfield == true && is001Datafield == false) {
				recordID = controlfieldText;
			}
		}


		if(localName.equals("subfield")) {
			String subfieldText = nodeContent.toString();
			datafield.setFieldvalue(subfieldText);
			allFields.add(datafield);

			if (is001Datafield == true && is001Controlfield == false) {
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

			counter = counter + 1;
			record.setMabfields(allFields);
			record.setRecordID(recordID);
			record.setRecordSYS(recordSYS);
			record.setIndexTimestamp(timeStamp);
			
			allRecords.add(record);
			
			print(this.print, "Indexing record " + ((recordID != null) ? recordID : recordSYS) + ", No. indexed: " + counter + "                 \r");

			/** Every n-th record, match the Mab-Fields to the Solr-Fields, write an appropirate object, loop through the object and index
			 * it's values to Solr, then empty all objects (clear and set to "null") to save memory and go on with the next n records.
			 * If there is a rest, do it in the endDocument()-Method. E. g. modulo is set to 100 and we have 733 records, but at this point,
			 * only 700 are indexed. The 33 remaining records will be indexed in endDocument() function.
			 */
			if (counter % NO_OF_DOCS == 0) {

				// Do the Matching and rewriting (see class "MatchingOperations"):
				List<Record> newRecordSet = matchingOps.matching(allRecords, listOfMatchingObjs);

				// Add to Solr-Index:
				this.solrAddRecordSet(sServer, newRecordSet);

				// Set all Objects to "null" to save memory
				allRecords.clear();
				allRecords = null;
				allRecords = new ArrayList<Record>();
				allFields.clear();
				allFields = null;
				newRecordSet = null;
			}
		}
	}

	
	/**
	 * Executed when encountering the end element of the XML file.
	 */
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
		allRecords.clear();
		allRecords = null;
		newRecordSet.clear();
		newRecordSet = null;
		listOfMatchingObjs.clear();
		listOfMatchingObjs = null;

	}


	/**
	 * Reads the content of the current XML element:
	 */
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		nodeContent += new String(ch, start, length);
	}


	/**
	 * This method contains the code that actually adds a set of Record objects
	 * (see Record class) to the specified Solr server.
	 */
	public void solrAddRecordSet(SolrServer sServer, List<Record> recordSet) {
		try {

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			for (Record record : recordSet) {

				// Create a document:
				SolrInputDocument doc = new SolrInputDocument();

				for (Mabfield mf : record.getMabfields()) {

					String fieldName = mf.getFieldname();
					String fieldValue = mf.getFieldvalue();
					
					// Add the fieldname and fieldvalue to the document:
					doc.addField(fieldName, fieldValue);
				}

				// Add the timestamp of indexing (it is the timstamp of the beginning of the indexing process - see betullam.akimporter.main.Main):
				doc.addField("indexTimestamp_str", record.getIndexTimestamp());

				// Add the document to the collection of documents:
				docs.add(doc);
			}

			if (docs.isEmpty() == false) {

				// Now add the collection of documents to Solr:
				sServer.add(docs);

				// Set "docs" to "null" (save memory):
				docs = null;
			}


		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	/**
	 * Unused methods of ContentHandler class.
	 * We just define them without any content.
	 */
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

	
	/**
	 * Prints the specified text to the console if "print" is true.
	 * 
	 * @param print	boolean: true if the text should be print
	 * @param text	String: a text message to print.
	 */
	private void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}

}