package betullam.akimporter.solrmab2.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class ChildsToParents {

	// General variables
	SolrServer sServer;
	String timeStamp;
	Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	Helper helper;
	int NO_OF_ROWS = 1000;
	int counter = 0;
	Set<String> parentAcs = new HashSet<String>();


	public ChildsToParents(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
		helper = new Helper(sServer, timeStamp);
	}



	public void addChildsToParents() {

		SolrDocumentList queryResults = helper.getCurrentlyIndexedChildRecords(true, null);

		// Get the number of documents that were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the AC Nos from the parents to a class variable of type "Set<String>" to get a list without duplicated
				// values to avoid an overhead
				lastDocId = setParentAcsFromWhichToUnlink(isFirstPage, lastDocId);
				
				// Set the documents for atomic updates to a class variable of type "Collection<SolrInputDocument>"
				setParentAtomicUpdateDocs();
				
				// Add documents from the class variable which was set before to Solr 
				helper.indexDocuments(docsForAtomicUpdates);
				
				// Set Collection<SolrInputDocument> to null and then to a fresh Collection to save memory
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
				parentAcs = null;
				parentAcs = new HashSet<String>();
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;
				
				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentAcsFromWhichToUnlink(isFirstPage, lastDocId);
				
				// Set the documents for atomic updates to a class variable of type "Collection<SolrInputDocument>"
				setParentAtomicUpdateDocs();
				
				// Add documents from the class variable which was set before to Solr 
				helper.indexDocuments(docsForAtomicUpdates);
				
				// Set Collection<SolrInputDocument> to null and then to a fresh Collection to save memory
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
				parentAcs = null;
				parentAcs = new HashSet<String>();
			}

			
			// Commit the changes
			try {
				this.sServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates = null;
				parentAcs = null;
				queryResults = null;
			}	
		}
	}


	// Add all parent AC numbers to a class variable to have no duplicated values because this would cause an overhead
	public String setParentAcsFromWhichToUnlink(boolean isFirstPage, String lastDocId) {

		// Variable for return value:
		String returnValue = null;

		SolrDocumentList childRecords = helper.getCurrentlyIndexedChildRecords(isFirstPage, lastDocId);
		String newLastDocId = childRecords.get(childRecords.size()-1).getFieldValue("id").toString();

		String docId = null;

		for (SolrDocument childRecord : childRecords) {
			String parentAc = helper.getParentAc(childRecord);
			if (parentAc != null) {
				parentAcs.add(parentAc);
			}
			docId = (childRecord.getFieldValue("id") != null) ? childRecord.getFieldValue("id").toString() : null;

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}

		return returnValue;
	}



	private void setParentAtomicUpdateDocs() {

		int noOfParentAcs = parentAcs.size();
		
		for (String parentAc : parentAcs) {

			counter = counter + 1;
			
			// Query all childs from a parent except the deleted ones
			SolrDocumentList nonDeletedChilds = getNonDeletedChildsByParentAc(parentAc);

			// Add all non deleted childs to it's parents:
			for (SolrDocument nonDeletedChild : nonDeletedChilds) {

				// Get record type
				String recordType = helper.getChildRecordType(nonDeletedChild);

				// Get info from child records
				String parentSys = (nonDeletedChild.getFieldValue("parentSYS_str") != null) ? nonDeletedChild.getFieldValue("parentSYS_str").toString() : null;
				String childSys = (nonDeletedChild.getFieldValue("sysNo_str") != null) ? nonDeletedChild.getFieldValue("sysNo_str").toString() : "0";
				String childAc = (nonDeletedChild.getFieldValue("acNo_str") != null) ? nonDeletedChild.getFieldValue("acNo_str").toString() : "0";
				String childTitle = (nonDeletedChild.getFieldValue("title") != null) ? nonDeletedChild.getFieldValue("title").toString() : "0";
				String childVolumeNo = "0";
				String childVolumeNoSort = "0";
				if (recordType.equals("multivolume")) {
					childVolumeNo = (nonDeletedChild.getFieldValue("volumeNo_str") != null) ? nonDeletedChild.getFieldValue("volumeNo_str").toString() : "0";
					childVolumeNoSort = (nonDeletedChild.getFieldValue("volumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("volumeNoSort_str").toString() : "0";
				} else if (recordType.equals("serialvolume")) {
					childVolumeNo = (nonDeletedChild.getFieldValue("serialVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNo_str").toString() : "0";
					childVolumeNoSort = (nonDeletedChild.getFieldValue("serialVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNoSort_str").toString() : "0";
				}
				String childEdition = (nonDeletedChild.getFieldValue("edition") != null) ? nonDeletedChild.getFieldValue("edition").toString() : "0";
				String childPublishDate = (nonDeletedChild.getFieldValue("publishDate") != null) ? nonDeletedChild.getFieldValue("publishDate").toString() : "0";

				
				if (parentSys != null) {
					
					// Set field names (for serial volume or multi-volume-work volume)
					Map<String, String> fieldNames = helper.getFieldNames(recordType);
					String fnChildSys = fieldNames.get("childSys");
					String fnChildAc = fieldNames.get("childAc");
					String fnChildTitle = fieldNames.get("childTitle");
					String fnChildVolNo = fieldNames.get("childVolNo");
					String fnChildVolNoSort = fieldNames.get("childVolNoSort");
					String fnChildEdition = fieldNames.get("childEdition");
					String fnChildPublishDate = fieldNames.get("childPublishDate");
					
					// Prepare parent record for atomic updates:
					SolrInputDocument linkedChild = null;
					linkedChild = new SolrInputDocument();
					linkedChild.setField("id", parentSys);

					// Set values for atomic update of parent record:
					Map<String, String> mapChildSYS = new HashMap<String, String>();
					mapChildSYS.put("add", childSys);
					linkedChild.setField(fnChildSys, mapChildSYS);

					Map<String, String> mapChildAC = new HashMap<String, String>();
					mapChildAC.put("add", childAc);
					linkedChild.setField(fnChildAc, mapChildAC);

					Map<String, String> mapChildTitle = new HashMap<String, String>();
					mapChildTitle.put("add", childTitle);
					linkedChild.setField(fnChildTitle, mapChildTitle);

					Map<String, String> mapChildVolumeNo = new HashMap<String, String>();
					mapChildVolumeNo.put("add", childVolumeNo);
					linkedChild.setField(fnChildVolNo, mapChildVolumeNo);

					Map<String, String> mapChildVolumeNoSort = new HashMap<String, String>();
					mapChildVolumeNoSort.put("add", childVolumeNoSort);
					linkedChild.setField(fnChildVolNoSort, mapChildVolumeNoSort);

					Map<String, String> mapChildEdition = new HashMap<String, String>();
					mapChildEdition.put("add", childEdition);
					linkedChild.setField(fnChildEdition, mapChildEdition);

					Map<String, String> mapChildPublishDate = new HashMap<String, String>();
					mapChildPublishDate.put("add", childPublishDate);
					linkedChild.setField(fnChildPublishDate, mapChildPublishDate);

					docsForAtomicUpdates.add(linkedChild);
					
					System.out.print("Linking child " + childSys + " to " + parentSys + ". Processing record no " + counter + " of " + noOfParentAcs + "           \r");
					System.out.print(StringUtils.repeat("\b", 130) + "\r");
				}
			}

		}
	}




	private SolrDocumentList getNonDeletedChildsByParentAc(String parentAc) {

		SolrDocumentList nonDeletedChildRecords = null;

		// New Solr query
		SolrQuery querynonDeletedChilds = new SolrQuery();

		// Add sorting
		querynonDeletedChilds.addSort(new SortClause("id", SolrQuery.ORDER.asc));

		// Set rows to a very high number so that we really get all child records
		// This should not be the bottleneck concerning performance because no parent records will have
		// such a high number of child volumes
		querynonDeletedChilds.setRows(500000);

		// Define a query for getting all documents. We get the deleted records with a filter query because of performance (see below)
		querynonDeletedChilds.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		querynonDeletedChilds.setFilterQueries("parentAC_str:"+parentAc+" || parentSeriesAC_str:"+parentAc, "-deleted_str:Y");

		// Set fields that should be given back from the query
		querynonDeletedChilds.setFields(
				"sysNo_str",
				"acNo_str",
				"title",
				"volumeNo_str",
				"volumeNoSort_str",
				"serialVolumeNo_str",
				"serialVolumeNoSort_str",
				"edition",
				"publishDate",
				"parentSYS_str",
				"parentAC_str",
				"parentSeriesAC_str"
				);


		try {
			nonDeletedChildRecords = sServer.query(querynonDeletedChilds).getResults();			
		} catch (SolrServerException e) {
			nonDeletedChildRecords = null;
			e.printStackTrace();
		}

		return nonDeletedChildRecords;
	}


}
