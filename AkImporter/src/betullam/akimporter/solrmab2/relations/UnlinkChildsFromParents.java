package betullam.akimporter.solrmab2.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class UnlinkChildsFromParents {

	// General variables
	SolrServer sServer;
	String timeStamp;
	Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	Helper helper;
	long noOfDocs = 0;
	int NO_OF_ROWS = 1000;
	int counter = 0;
	Map<String, String> parentInfos = new HashMap<String, String>();


	public UnlinkChildsFromParents(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
		helper = new Helper(sServer, timeStamp);
	}




	public void unlinkChildsFromParents() {

		SolrDocumentList queryResults = helper.getCurrentlyIndexedChildRecords(true, null);

		// Show how many documents were found
		noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the AC Nos and record types from the parents to a class variable of type "Map<String, String>" to get a
				// list without duplicated values to avoid an overhead
				lastDocId = setParentAcsFromWhichToUnlink(isFirstPage, lastDocId);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;
				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentAcsFromWhichToUnlink(isFirstPage, lastDocId);
			}

			// Set the documents for atomic updates to a class variable of type "Collection<SolrInputDocument>" and add that to Solr
			setParentAtomicUpdateDocs();


			// Commit the changes
			try {
				this.sServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates = null;
				queryResults = null;
			}
		}
	}



	// Add all parent AC numbers to a class variable to have no duplicated values because this would cause an overhead
	public String setParentAcsFromWhichToUnlink(boolean isFirstPage, String lastDocId) {

		// Variable for return value:
		String returnValue = null;

		SolrDocumentList childRecords = helper.getCurrentlyIndexedChildRecords(isFirstPage, lastDocId);

		if (childRecords.size() > 0) {
			String newLastDocId = childRecords.get(childRecords.size()-1).getFieldValue("id").toString();

			String docId = null;

			for (SolrDocument childRecord : childRecords) {
				String parentAc = helper.getParentAc(childRecord);
				String recordType = helper.getChildRecordType(childRecord);
				if (parentAc != null) {
					parentInfos.put(parentAc, recordType);
				}
				docId = (childRecord.getFieldValue("id") != null) ? childRecord.getFieldValue("id").toString() : null;

				// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
				if (docId.equals(newLastDocId)) {
					returnValue = docId;
				}
			}
		}

		return returnValue;
	}



	private void setParentAtomicUpdateDocs() {

		int noOfParents = parentInfos.size();
		int wholeLoops = (noOfParents/NO_OF_ROWS);

		if (noOfParents > 0) {

			for (Entry<String, String> parentInfo : parentInfos.entrySet()) {
				
				String parentAc = parentInfo.getKey();
				String recordType = parentInfo.getValue();

				SolrDocument parentRecord = helper.getParentRecord(parentAc);
				SolrInputDocument deleteChild = null;

				if (parentRecord != null) {

					String parentSys = (parentRecord.getFieldValue("id") != null) ? parentRecord.getFieldValue("id").toString() : "0";

					Map<String, String> fieldNames = helper.getFieldNames(recordType);
					String fnChildSys = fieldNames.get("childSys");
					String fnChildAc = fieldNames.get("childAc");
					String fnChildTitle = fieldNames.get("childTitle");
					String fnChildVolNo = fieldNames.get("childVolNo");
					String fnChildVolNoSort = fieldNames.get("childVolNoSort");
					String fnChildEdition = fieldNames.get("childEdition");
					String fnChildPublishDate = fieldNames.get("childPublishDate");


					// Prepare parent record for atomic updates:
					deleteChild = new SolrInputDocument();
					deleteChild.setField("id", parentSys);

					// Unlink all child records:
					Map<String, String> mapRemSYS = new HashMap<String, String>();
					mapRemSYS.put("set", null);
					deleteChild.setField(fnChildSys, mapRemSYS);

					Map<String, String> mapRemAC = new HashMap<String, String>();
					mapRemAC.put("set", null);
					deleteChild.setField(fnChildAc, mapRemAC);

					Map<String, String> mapRemTitle = new HashMap<String, String>();
					mapRemTitle.put("set", null);
					deleteChild.setField(fnChildTitle, mapRemTitle);

					Map<String, String> mapRemVolumeNo = new HashMap<String, String>();
					mapRemVolumeNo.put("set", null);
					deleteChild.setField(fnChildVolNo, mapRemVolumeNo);

					Map<String, String> mapRemVolumeNoSort = new HashMap<String, String>();
					mapRemVolumeNoSort.put("set", null);
					deleteChild.setField(fnChildVolNoSort, mapRemVolumeNoSort);

					Map<String, String> mapRemEdition = new HashMap<String, String>();
					mapRemEdition.put("set", null);
					deleteChild.setField(fnChildEdition, mapRemEdition);

					Map<String, String> mapRemPublishDate = new HashMap<String, String>();
					mapRemPublishDate.put("set", null);
					deleteChild.setField(fnChildPublishDate, mapRemPublishDate);

					docsForAtomicUpdates.add(deleteChild);
				}
				
				counter = counter + 1;

				// Add documents from the class variable which was set before to Solr
				if (counter % NO_OF_ROWS == 0) { // Every n-th record, add documents to solr
					helper.indexDocuments(docsForAtomicUpdates);
					docsForAtomicUpdates = null; // Set to null to save memory
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				} else if (((wholeLoops*NO_OF_ROWS)+1) == counter) { // The remainding documents (if division with NO_OF_ROWS 
					helper.indexDocuments(docsForAtomicUpdates);
					docsForAtomicUpdates = null; // Set to null to save memory
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}

				System.out.print("Unlinking childs from parent " + parentAc + ". Processing record no " + counter  + " of " + noOfParents + "\r");
			}
		}

	}


}
