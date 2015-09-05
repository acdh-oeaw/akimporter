package betullam.akimporter.solrmab2.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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
	int NO_OF_ROWS = 500;
	List<String> parentAcs = new ArrayList<String>();


	public UnlinkChildsFromParents(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
		helper = new Helper(sServer, timeStamp);
	}




	public void unlinkChildsFromParents() {

		SolrDocumentList queryResults = helper.getCurrentlyIndexedChildRecords(true, null);

		// Show how many documents were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Clear query results to save memory. We don't need it anymore.
			queryResults.clear();
			queryResults = null;

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

			try {
				this.sServer.commit(); // Commit the changes
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				// Save memory
				parentAcs.clear();
				parentAcs = null;
				docsForAtomicUpdates.clear();
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
				String[] arrParentAcsSingleChild = helper.getParentAcsFromSingleChild(childRecord);
				//String recordType = helper.getChildRecordType(childRecord);
				if (arrParentAcsSingleChild != null && arrParentAcsSingleChild.length > 0) {
					for (String parentAc : arrParentAcsSingleChild) {
						parentAcs.add(parentAc);
					}
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

		int counter = 0;
		int noOfParents = parentAcs.size();
		
		if (noOfParents > 0) {

			for (String parentAc : parentAcs) {
				
				SolrDocument parentRecord = helper.getParentRecord(parentAc);
				
				if (parentRecord != null) {

					String parentSys = (parentRecord.getFieldValue("id") != null) ? parentRecord.getFieldValue("id").toString() : "0";
					
					// Prepare parent record for atomic updates:
					SolrInputDocument deleteChild = null;
					deleteChild = new SolrInputDocument();
					deleteChild.setField("id", parentSys);
					
					// Unlink all child records:
					Map<String, List<String>> mapRemChildType = new HashMap<String, List<String>>();
					mapRemChildType.put("set", null);
					deleteChild.setField("childType_str_mv", mapRemChildType);
					
					Map<String, String> mapRemSYS = new HashMap<String, String>();
					mapRemSYS.put("set", null);
					deleteChild.setField("childSYS_str_mv", mapRemSYS);
					
					Map<String, String> mapRemAC = new HashMap<String, String>();
					mapRemAC.put("set", null);
					deleteChild.setField("childAC_str_mv", mapRemAC);

					Map<String, String> mapRemTitle = new HashMap<String, String>();
					mapRemTitle.put("set", null);
					deleteChild.setField("childTitle_str_mv", mapRemTitle);

					Map<String, String> mapRemVolumeNo = new HashMap<String, String>();
					mapRemVolumeNo.put("set", null);
					deleteChild.setField("childVolumeNo_str_mv", mapRemVolumeNo);

					Map<String, String> mapRemVolumeNoSort = new HashMap<String, String>();
					mapRemVolumeNoSort.put("set", null);
					deleteChild.setField("childVolumeNoSort_str_mv", mapRemVolumeNoSort);

					Map<String, String> mapRemEdition = new HashMap<String, String>();
					mapRemEdition.put("set", null);
					deleteChild.setField("childEdition_str_mv", mapRemEdition);

					Map<String, String> mapRemPublishDate = new HashMap<String, String>();
					mapRemPublishDate.put("set", null);
					deleteChild.setField("childPublishDate_str_mv", mapRemPublishDate);
					
					docsForAtomicUpdates.add(deleteChild);
				}
				
				counter = counter + 1;
				
				// Add documents from the class variable which was set before to Solr
				if (counter % NO_OF_ROWS == 0) { // Every n-th record, add documents to solr
					helper.indexDocuments(docsForAtomicUpdates);
					docsForAtomicUpdates.clear(); // Clear to save memory
					docsForAtomicUpdates = null; // Set to null to save memory
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				} else if (counter >= noOfParents) { // The remainding documents
					helper.indexDocuments(docsForAtomicUpdates);
					docsForAtomicUpdates.clear(); // Clear to save memory
					docsForAtomicUpdates = null; // Set to null to save memory
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}

				System.out.print("Unlinking childs from parent " + parentAc + ". Processing record no " + counter  + " of " + noOfParents + "        \r");
				System.out.print(StringUtils.repeat("\b", 130) + "\r");
			}

		}

	}


}
