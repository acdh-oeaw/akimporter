package betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
	int NO_OF_ROWS = 500;
	int CHILD_INDEX_RATE = 250;
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

				// Set the AC Nos from the parents to a class variable of type "Set<String>" to get a list without duplicated
				// values to avoid an overhead
				lastDocId = setParentAcsForLinking(isFirstPage, lastDocId);			
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentAcsForLinking(isFirstPage, lastDocId);
			}

			setParentAtomicUpdateDocs();

			// Commit the changes
			try {
				this.sServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				parentAcs.clear();
				parentAcs = null;
				queryResults = null;
			}			
		}
	}


	// Add all parent AC numbers to a class variable to have no duplicated values because this would cause an overhead
	public String setParentAcsForLinking(boolean isFirstPage, String lastDocId) {

		// Variable for return value:
		String returnValue = null;

		SolrDocumentList childRecords = helper.getCurrentlyIndexedChildRecords(isFirstPage, lastDocId);
		String newLastDocId = childRecords.get(childRecords.size()-1).getFieldValue("id").toString();

		String docId = null;

		for (SolrDocument childRecord : childRecords) {
			String[] parentAcsFromChild = helper.getParentAcsFromSingleChild(childRecord);
			if (parentAcsFromChild != null && parentAcsFromChild.length > 0) {
				for (String parentAc : parentAcsFromChild) {
					parentAcs.add(parentAc);
				}
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

		int counter = 0;
		int noOfParents = parentAcs.size();

		if (noOfParents > 0) {

			for (String parentAc : parentAcs) {

				Set<String> setParentSYSs = new HashSet<String>();
				List<String> childTypes = new ArrayList<String>();
				List<String> childSYSs = new ArrayList<String>();
				List<String> childACs = new ArrayList<String>();
				List<String> childTitles = new ArrayList<String>();
				List<String> childVolumeNos = new ArrayList<String>();
				List<String> childVolumeNosSort = new ArrayList<String>();
				List<String> childEditions = new ArrayList<String>();
				List<String> childPublishDates = new ArrayList<String>();

				// Query all childs from a parent except the deleted ones
				SolrDocumentList nonDeletedChilds = getNonDeletedChildsByParentAc(parentAc);


				// Add all non deleted childs to it's parents:
				for (SolrDocument nonDeletedChild : nonDeletedChilds) {

					// Get type of child record (multivolume, serialvolume, etc.)
					String childType = helper.getChildRecordType(nonDeletedChild);

					// Get info from child records
					String[] arrParentSYSs = (nonDeletedChild.getFieldValues("parentSYS_str_mv") != null) ? nonDeletedChild.getFieldValues("parentSYS_str_mv").toArray(new String[0]) : null;
					String childSys = (nonDeletedChild.getFieldValue("sysNo_str") != null) ? nonDeletedChild.getFieldValue("sysNo_str").toString() : "0";
					String childAc = (nonDeletedChild.getFieldValue("acNo_str") != null) ? nonDeletedChild.getFieldValue("acNo_str").toString() : "0";
					String childTitle = (nonDeletedChild.getFieldValue("title") != null) ? nonDeletedChild.getFieldValue("title").toString() : "0";
					String childVolumeNo = "0";
					String childVolumeNoSort = "0";
					if (childType.equals("multivolume")) {
						childVolumeNo = (nonDeletedChild.getFieldValue("multiVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNo_str").toString() : "0";
						childVolumeNoSort = (nonDeletedChild.getFieldValue("multiVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNoSort_str").toString() : "0";
					} else if (childType.equals("serialvolume")) {
						childVolumeNo = (nonDeletedChild.getFieldValue("serialVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNo_str").toString() : "0";
						childVolumeNoSort = (nonDeletedChild.getFieldValue("serialVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNoSort_str").toString() : "0";
					} else if (childType.equals("article")) {
						childVolumeNo = "0";
						childVolumeNoSort = "0";
					}
					String childEdition = (nonDeletedChild.getFieldValue("edition") != null) ? nonDeletedChild.getFieldValue("edition").toString() : "0";
					String childPublishDate = (nonDeletedChild.getFieldValue("publishDate") != null) ? nonDeletedChild.getFieldValue("publishDate").toString() : "0";

					if (arrParentSYSs != null && arrParentSYSs.length > 0) {
						
						// 1. Put Infos from all childs of this parent into Lists, except for the parent SYSs
						childTypes.add(childType);
						childSYSs.add(childSys);
						childACs.add(childAc);
						childTitles.add(childTitle);
						childVolumeNos.add(childVolumeNo);
						childVolumeNosSort.add(childVolumeNoSort);
						childEditions.add(childEdition);
						childPublishDates.add(childPublishDate);

						// 2. Put the parent SYSs from all childs into a Set<String> without duplicates
						for (String parentSys : arrParentSYSs) {
							setParentSYSs.add(parentSys);
						}						
					}
				}
				
				
				// 3. Iterate over the Set<String> and create documents for indexing.
				for (String parentSys : setParentSYSs) {
					
					// Prepare parent record for atomic updates:
					SolrInputDocument linkedChild = null;
					linkedChild = new SolrInputDocument();
					linkedChild.setField("id", parentSys);
					
					// Set values for atomic update of parent record:
					Map<String, List<String>> mapChildType = new HashMap<String, List<String>>();
					mapChildType.put("set", childTypes);
					linkedChild.setField("childType_str_mv", mapChildType);
					
					Map<String, List<String>> mapChildSYS = new HashMap<String, List<String>>();
					mapChildSYS.put("set", childSYSs);
					linkedChild.setField("childSYS_str_mv", mapChildSYS);

					Map<String, List<String>> mapChildAC = new HashMap<String, List<String>>();
					mapChildAC.put("set", childACs);
					linkedChild.setField("childAC_str_mv", mapChildAC);

					Map<String, List<String>> mapChildTitle = new HashMap<String, List<String>>();
					mapChildTitle.put("set", childTitles);
					linkedChild.setField("childTitle_str_mv", mapChildTitle);

					Map<String, List<String>> mapChildVolumeNo = new HashMap<String, List<String>>();
					mapChildVolumeNo.put("set", childVolumeNos);
					linkedChild.setField("childVolumeNo_str_mv", mapChildVolumeNo);

					Map<String, List<String>> mapChildVolumeNoSort = new HashMap<String, List<String>>();
					mapChildVolumeNoSort.put("set", childVolumeNosSort);
					linkedChild.setField("childVolumeNoSort_str_mv", mapChildVolumeNoSort);

					Map<String, List<String>> mapChildEdition = new HashMap<String, List<String>>();
					mapChildEdition.put("set", childEditions);
					linkedChild.setField("childEdition_str_mv", mapChildEdition);

					Map<String, List<String>> mapChildPublishDate = new HashMap<String, List<String>>();
					mapChildPublishDate.put("set", childPublishDates);
					linkedChild.setField("childPublishDate_str_mv", mapChildPublishDate);

					docsForAtomicUpdates.add(linkedChild);
				}

				counter = counter + 1;

				// Add documents from the class variable which was set before to Solr
				if (counter % CHILD_INDEX_RATE == 0) { // Every n-th record, add documents to solr
					helper.indexDocuments(docsForAtomicUpdates);
					docsForAtomicUpdates.clear(); // Clear to save memory
					docsForAtomicUpdates = null; // Set to null to save memory
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				} else if (counter >= noOfParents) { // The remainding documents (if division with NO_OF_ROWS 
					helper.indexDocuments(docsForAtomicUpdates);
					docsForAtomicUpdates.clear(); // Clear to save memory
					docsForAtomicUpdates = null; // Set to null to save memory
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}


				System.out.print("Linking childs to parent " + parentAc + ". Processing record no " + counter  + " of " + noOfParents + "            \r");
				System.out.print(StringUtils.repeat("\b", 130) + "\r");
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
		querynonDeletedChilds.setFilterQueries("parentMultiAC_str"+parentAc+" || parentSeriesAC_str_mv:"+parentAc+" || articleParentAC_str:"+parentAc, "-deleted_str:Y");

		// Set fields that should be given back from the query
		querynonDeletedChilds.setFields(
				"sysNo_str",
				"acNo_str",
				"title",
				"multiVolumeNo_str",
				"multiVolumeNoSort_str",
				"serialVolumeNo_str",
				"serialVolumeNoSort_str",
				"edition",
				"publishDate",
				"parentSYS_str_mv",
				"parentMultiAC_str",
				"parentSeriesAC_str_mv",
				"articleParentAC_str"
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
