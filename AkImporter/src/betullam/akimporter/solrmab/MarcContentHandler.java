package betullam.akimporter.solrmab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class MarcContentHandler implements ContentHandler {

	MatchingOperations matchingOps = new MatchingOperations();
	List<Controlfield> allControlfields;
	List<Datafield> allDatafields;
	List<Record> allRecords;
	List<Subfield> allSubfieldsOfNode;
	List<Mabfield> allMabfields;
	private String nodeContent;
	private RecordSet recordSet;
	private Record record;
	private Mabfield controlfield;
	private Datafield datafield;
	private Subfield subfield;
	private Mabfield mabfield;
	private int counter;
	private List<MatchingObject> listOfMatchingObjs;
	private SolrServer sServer;
	private boolean isPublisherData;
	private String startElement;
	private String recordID;
	private boolean is001;
	private String satztypOfRecord;
	private String satztypToIndex;
	private boolean isFMT;
	private boolean skipRecord;
	private long startTime;
	private boolean print = true;

	public MarcContentHandler(List<MatchingObject> listOfMatchingObjs, HttpSolrServer solrServer, String satztypToIndex, boolean isPublisherData, long startTime, boolean print) {
		this.listOfMatchingObjs = listOfMatchingObjs;
		this.sServer = solrServer;
		this.isPublisherData = isPublisherData;
		this.satztypToIndex = satztypToIndex;
		this.startTime = startTime;
		this.print = print;
	}



	@Override
	public void startDocument() throws SAXException {
		// On document-start, crate new list to hold all parsed AlephMARCXML-records and a RecordSet-object which should hold that list:
		counter = 0;
		allRecords = new ArrayList<Record>();

		// If data are from Aleph Publisher, start element for SAX-Parser must be "metadata":
		this.startElement = (isPublisherData == true) ? "metadata" : "record";
	}


	// Read and process parameters of XML-Tags and create objects here. Content (text) is processed in endElement()!
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attribs) throws SAXException {


		// Empty node content (text), else, there will be problems with html-encoded characters (&lt;) at character()-method:
		nodeContent = "";

		// If the parser encounters the start of the "record"-tag, create new Lists to hold the fields of this record and a new record-object to add these lists:
		if(localName.equals(startElement)) {
			allControlfields = new ArrayList<Controlfield>();
			allDatafields = new ArrayList<Datafield>();
			allMabfields = new ArrayList<Mabfield>();
			record = new Record();
		}


		// If the record is not the "satztyp" (MH or MU) which should be indexed (all MH must be indexed before MU), skip the record. This saves a lot of time!
		// Explanation: All MH records must be indexed before MU records, so that MH records could be extended with data from MU records. So if an MU record is
		// encountered while the indexing-run for MH records, it can be skiped. If an MH record is encountered whilst the index-run for MU records, that one 
		// will be skiped.
		if (skipRecord == false) {

			// If the parser encounters the start of the "controlfield"-tag, create a new controlfield-object, get the XML-attributes, set them on the object and add it to the "record"-object:
			if(localName.equals("controlfield")) {
				String tag = attribs.getValue("tag");
				controlfield = new Mabfield();
				controlfield.setFieldname(tag);
				isFMT = (tag.equals("FMT")) ? true : false;
			}

			if(localName.equals("datafield")) {
				allSubfieldsOfNode = new ArrayList<Subfield>();
				String tag = attribs.getValue("tag");
				String ind1 = attribs.getValue("ind1");
				String ind2 = attribs.getValue("ind2");

				// Set empty indicators with a character, so that the string to match against is always of the same length, e. g. "311$ab$c".
				// This prevents errors with normal ALEPH Exports (via print_03) and ALEPH Publisher exports.
				ind1 = (ind1.trim().isEmpty() == true) ? "*" : ind1;
				ind2 = (ind2.trim().isEmpty() == true) ? "*" : ind2;

				datafield = new Datafield();
				datafield.setTag(tag);
				datafield.setInd1(ind1);
				datafield.setInd2(ind2);

				is001 = (tag.equals("001")) ? true : false;
			}

			if(localName.equals("subfield")) {
				String code = attribs.getValue("code");
				subfield = new Subfield();
				subfield.setCode(code);

				String trimmedFieldNo = datafield.getTag().trim();
				String trimedIndicators = datafield.getInd1().trim() + datafield.getInd2().trim();
				String trimmedSubfield = subfield.getCode().trim();
				String strFieldNo = (trimmedFieldNo != null && !trimmedFieldNo.isEmpty()) ? trimmedFieldNo : "000";
				String strIndicators = (trimedIndicators != null && !trimedIndicators.isEmpty()) ? trimedIndicators : "-";
				String strSubfield = (trimmedSubfield != null && !trimmedSubfield.isEmpty()) ? trimmedSubfield : "-";
				String mabFieldname = strFieldNo + "$" + strIndicators + "$" + strSubfield;

				mabfield = new Mabfield();
				mabfield.setFieldname(mabFieldname);
			}

		}
	}


	// Process content (text) of current XML-node (not working in startElement()) - see also characters():
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {

		if(localName.equals("controlfield")) {
			String controlfieldText = nodeContent.toString();
			controlfield.setFieldvalue(controlfieldText);
			allMabfields.add(controlfield);

			// Make the following check only if controlfield contains "MH" or "MU". Skip all other FMT-fields, e. g. "ML$$2A001"
			if (isFMT == true) {
				if (controlfieldText.equals("MH") || controlfieldText.equals("MU")) {
					satztypOfRecord = controlfieldText;
					skipRecord = (satztypOfRecord.equals(satztypToIndex)) ? false : true;
				}
			}

			if (controlfieldText.equals(satztypToIndex)) {
				counter += 1;
			}
		}

		// If the record is not the "satztyp" (MH or MU) which should be indexed (all MH must be indexed before MU), skip the record. This saves a lot of time!
		// Explanation: All MH records must be indexed before MU records, so that MH records could be extended with data from MU records. So if an MU record is
		// encountered while the indexing-run for MH records, it can be skiped. If an MH record is encountered whilst the index-run for MU records, that one 
		// will be skiped.
		if (skipRecord == false) {

			if(localName.equals("subfield")) {
				String subfieldText = nodeContent.toString();
				subfield.setSubfieldText(subfieldText);
				allSubfieldsOfNode.add(subfield);

				String mabFieldvalue = subfield.getSubfieldText().trim();
				mabfield.setFieldvalue(mabFieldvalue);
				allMabfields.add(mabfield);

				if (is001 == true) {
					recordID = mabFieldvalue;
				}
			}

			if(localName.equals("datafield")) {
				datafield.setSubfields(allSubfieldsOfNode);
				allDatafields.add(datafield);
			}

			// If the parser encounters the end of the "record"- or "metadata"-tag, add all leader-, controlfield- and datafield-objects to the record-object and add the record-object to the list of all records:
			if(localName.equals(startElement)) {
				record.setControlfields(allControlfields);
				record.setDatafields(allDatafields);
				record.setMabfields(allMabfields);
				record.setSatztyp(satztypOfRecord);
				record.setRecordID(recordID);
				allRecords.add(record);

				/** Every n record, match the Mab-Fields to the Solr-Fields, write an appropirate object, loop through the object and
				 * index it's values to Solr, then empty all objects (set to "null") to save memory and go on with the next n records.
				 * If there is a rest, do it in the endDocument()-Method. E. g. modulo is set to 100 and we have 733 records, but now
				 * only 700 are indexed! The 33 remaining records will be indexed in endDocument()-Method;
				 */

				if (counter % 300 == 0) {

					// Add the list of all records to the recordSet-Object:
					recordSet = new RecordSet();
					recordSet.setRecords(allRecords);

					// Do the Matching and rewriting (see class "MatchingOperations"):
					RecordSet newRecordSet = matchingOps.matching(recordSet, listOfMatchingObjs);

					// Add to Solr-Index:
					this.solrAddRecordSet(sServer, newRecordSet);
					//long opStartTime = System.nanoTime();
					//System.out.println(getDuration(opStartTime));

					// Set all Objects to "null" to save memory
					// TODO: Are the followoing really all objects which should be set to "null"?
					allRecords = null;
					allRecords = new ArrayList<Record>();
					recordSet = null;
					newRecordSet = null;

					// Print time:
					long endTime = System.nanoTime();
					long duration = endTime - startTime;
					Double hours = Math.floor(TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS) / 3600);
					Double minutes = Math.floor((TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS) / 60) % 60);
					Long seconds = TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS) % 60;

					print(counter + " " + satztypToIndex + " records (" + hours.shortValue()+"h"+minutes.shortValue()+"m"+seconds.shortValue()+"s)");
					print("\n");
				}
			}

		}

	}


	@Override
	public void endDocument() throws SAXException {

		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		//+++++++++++++++ Add the remaining rest of the records to the index (see modulo-operation with "%"-operator in endElement()) +++++++++++++++//
		//+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//

		// Add the list of all records to the recordSet-Object:
		recordSet = new RecordSet();
		recordSet.setRecords(allRecords);

		// Do the Matching and rewriting (see class "MatchingOperations"):
		RecordSet newRecordSet = matchingOps.matching(recordSet, listOfMatchingObjs);

		// Add to Solr-Index:
		this.solrAddRecordSet(sServer, newRecordSet);

		// Set all Objects to "null" to save memory
		// TODO: Are the followoing really all objects which should be set to "null"?
		allRecords = null;
		recordSet = null;
		newRecordSet = null;	

		if (isPublisherData == false) {
			print("\n-------------------------------------------\n");
			print("Number of records indexed: " + counter);
			print("\n###############################################################################\n\n");
		}

	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		// Reads the content of the current XML-node:
		nodeContent += new String(ch, start, length);
	}


	public void solrAddRecordSet(SolrServer sServer, RecordSet recordSet) {
		try {

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
			Collection<SolrInputDocument> mhAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

			for (Record record : recordSet.getRecords()) {

				String satztyp = record.getSatztyp();

				if (satztyp.equals(satztypToIndex)) {

					// Create a document:
					SolrInputDocument doc = new SolrInputDocument();

					// Make solr document for atomic updates of MH records with data from MU records:
					SolrInputDocument mhAtomicUpdateDoc = new SolrInputDocument();

					// Variables for atomic updates of MH record:
					String muAC = "0";
					String muSYS = "0";
					String muParentAC = "0";
					String muTitle = "0";
					String muVolumeNo = "0";
					String muEdition = "0";
					String muPublishDate = "0";

					// Variables for additional data from MH record for MU record
					String mhTitle = null;
					String mhSYS = null;

					// Variables for results of Solr-Queries:
					SolrDocument resultDoc = null;
					SolrDocumentList resultDocList = null;


					for (Mabfield mf : record.getMabfields()) {

						String fieldName = mf.getFieldname();
						String fieldValue = mf.getFieldvalue();

						// Add the fieldname and fieldvalue to the document:
						doc.addField(fieldName, fieldValue);

						// If it's an MU-record, gather some information so we can add them to the MH-record later:
						if (satztyp.equals("MU")) {

							// Get the AC-No from the MH record:
							if (fieldName.equals("parentAC_str_mv")) {
								muParentAC = fieldValue;
							}

							// Get all data that should be indexed for the MH record of this MU record:
							if (fieldName.equals("acNo_str")) {
								muAC = fieldValue;
							}
							if (fieldName.equals("sysNo_str")) {
								muSYS = fieldValue;
							}
							if (fieldName.equals("title")) {
								muTitle = fieldValue;
							}
							if (fieldName.equals("volumeNo_str_mv")) {
								muVolumeNo = fieldValue;
							}
							if (fieldName.equals("edition")) {
								muEdition = fieldValue;
							}
							if (fieldName.equals("publishDate")) {
								muPublishDate = fieldValue;
							}
						}
					}

					// Link MH and MU records togehter (add data from MH record to MU record and vice versa)
					if (satztyp.equals("MU")) {

						// Get MH record of current MU record, so that we can add values to it via atomic updates.
						// First, query Solr Server and find out if record/solr-document with the given AC-No. of parent exists:
						SolrQuery query = new SolrQuery();
						query.setQuery( "acNo_str:" + muParentAC );
						query.setFields("id", "title"); // Set fields that should be given back from the query
						QueryResponse rsp = sServer.query(query); // Execute query
						resultDocList = rsp.getResults(); // Get document-list from query result

						// Check if the parent record exists. If yes, add data from it to the current MU record.
						if (resultDocList != null && resultDocList.getNumFound() > 0) { // Parent record exists

							// Add data from MH record to current MU record:
							resultDoc = resultDocList.get(0); // Get first document from query result (there should be only one!)
							mhTitle = (resultDoc.getFieldValue("title") != null) ? resultDoc.getFieldValue("title").toString() : "0";
							mhSYS = (resultDoc.getFieldValue("id") != null) ? resultDocList.get(0).getFieldValue("id").toString() : "0";
							doc.addField("parentTitle_str_mv", mhTitle);
							doc.addField("parentSYS_str", mhSYS);

							// Prepare MH record for atomic updates:
							mhAtomicUpdateDoc.addField("id", mhSYS);

							// Add values for atomic update:
							Map<String, String> mapMuAC = new HashMap<String, String>();
							mapMuAC.put("add", muAC);
							mhAtomicUpdateDoc.setField("childAC_str_mv", mapMuAC);

							Map<String, String> mapMuSYS = new HashMap<String, String>();
							mapMuSYS.put("add", muSYS);
							mhAtomicUpdateDoc.setField("childSYS_str_mv", mapMuSYS);

							Map<String, String> mapMuTitle = new HashMap<String, String>();
							mapMuTitle.put("add", muTitle);
							mhAtomicUpdateDoc.setField("childTitle_str_mv", mapMuTitle);

							Map<String, String> mapMuVolumeNo = new HashMap<String, String>();
							mapMuVolumeNo.put("add", muVolumeNo);
							mhAtomicUpdateDoc.setField("childVolumeNo_str_mv", mapMuVolumeNo);

							Map<String, String> mapMuEdition = new HashMap<String, String>();
							mapMuEdition.put("add", muEdition);
							mhAtomicUpdateDoc.setField("childEdition_str_mv", mapMuEdition);

							Map<String, String> mapMuPublishDate = new HashMap<String, String>();
							mapMuPublishDate.put("add", muPublishDate);
							mhAtomicUpdateDoc.setField("childPublishDate_str_mv", mapMuPublishDate);

							// Add all values of MU child record to MH parent record:
							mhAtomicUpdateDocs.add(mhAtomicUpdateDoc);
						}

					}

					// Add the document to the collection of documents:
					docs.add(doc);

					print("Indexing record with ID: " + doc.getFieldValue("id") + " (" + record.getSatztyp() + ")");
				}


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


	public RecordSet getRecordSet() {
		return this.recordSet;
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

	public void printToConsole(RecordSet recordSet) {
		// Print the RecordSet to the console:
		if (print) {
			for (Record record : recordSet.getRecords()) {
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
