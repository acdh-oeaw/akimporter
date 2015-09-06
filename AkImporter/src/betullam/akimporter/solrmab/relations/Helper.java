package betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class Helper {


	// General variables
	SolrServer sServer;
	String timeStamp;
	int NO_OF_ROWS = 500;
	int rowCounter = 0;

	// Field name variables
	String fnChildSys = null;
	String fnChildAc = null;
	String fnChildTitle = null;
	String fnChildVolNo = null;
	String fnChildVolNoSort = null;
	String fnChildEdition = null;
	String fnChildPublishDate = null;


	public Helper(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
	}




	public SolrDocumentList getCurrentlyIndexedChildRecords(boolean isFirstPage, String lastDocId) {

		// Set up variable
		SolrDocumentList queryResult = null;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(NO_OF_ROWS);

		// Add sorting (more efficient for deep paging)
		query.addSort(SolrQuery.SortClause.asc("id"));

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		if (isFirstPage) { // No range filter on first page
			query.setFilterQueries("parentMultiAC_str:* || parentSeriesAC_str_mv:* || articleParentAC_str:*", "indexTimestamp_str:"+this.timeStamp, "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			query.setStart(1);
			query.setFilterQueries("parentMultiAC_str:* || parentSeriesAC_str_mv:* || articleParentAC_str:*", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_str", "parentSYS_str", "parentMultiAC_str", "parentSeriesAC_str_mv", "articleParentAC_str");


		try {
			// Execute query and get results
			queryResult = sServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
	}





	public String getChildRecordType(SolrDocument record) {
		String childRecordType = null;

		if (record.getFieldValue("parentMultiAC_str") != null) {
			childRecordType = "multivolume";
		} else if (record.getFieldValue("parentSeriesAC_str_mv") != null) {
			childRecordType = "serialvolume";
		} else if (record.getFieldValue("articleParentAC_str") != null) {
			childRecordType = "article";
		} else {
			System.err.println("\nRecord: " + record.getFieldValue("id"));
			System.err.println("Returnd fields in solr query must include the fields \"parentMultiAC_str\",  \"parentSeriesAC_str_mv\" and \"articleParentAC_str\".");
			return null;
		}

		return childRecordType;
	}


	public Set<String> getDedupParentAcsFromMultipleChilds(SolrDocumentList childDocumentList) {
		Set<String> parentAcs = null;
		if (childDocumentList != null && !childDocumentList.isEmpty()) {
			parentAcs = new HashSet<String>();
			for (SolrDocument child : childDocumentList) {
				String[] arrParentAcs = getParentAcsFromSingleChild(child);
				if (arrParentAcs.length > 0) {
					for (String parentAc : arrParentAcs) {
						parentAcs.add(parentAc);
					}
				}
			}
		}
		return parentAcs;
	}

	public String[] getParentAcsFromSingleChild(SolrDocument childRecord) {
		String[] parentAcs = null;

		if (childRecord != null) {
			String childRecordType = getChildRecordType(childRecord);
			if (childRecordType != null) {
				if (childRecordType.equals("multivolume")) {
					parentAcs = childRecord.getFieldValues("parentMultiAC_str").toArray(new String[0]);
				} else if (childRecordType.equals("serialvolume")) {
					parentAcs = childRecord.getFieldValues("parentSeriesAC_str_mv").toArray(new String[0]);
				} else if (childRecordType.equals("article")) {
					parentAcs = childRecord.getFieldValues("articleParentAC_str").toArray(new String[0]);
				}
			}
		}
		return parentAcs;
	}

	public SolrDocument getParentRecord(String parentAc) {
		SolrDocument parentRecord = null;
		SolrQuery queryParent = new SolrQuery(); // New Solr query
		queryParent.setQuery("acNo_str:"+parentAc); // Define a query
		queryParent.setFields("id", "title"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = sServer.query(queryParent).getResults();
			if (resultList.getNumFound() > 0 && resultList != null) {
				parentRecord = sServer.query(queryParent).getResults().get(0);// Get parent document (there should only be one)
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return parentRecord;
	}

	public List<SolrDocument> getParentRecords(String[] parentAcs) {
		List<SolrDocument> parentRecords = null;

		if (parentAcs != null) {
			parentRecords = new ArrayList<SolrDocument>();
			for (String parentAc : parentAcs) {
				SolrDocument sdParentRecord = this.getParentRecord(parentAc);
				if (sdParentRecord != null) {
					parentRecords.add(sdParentRecord);
				}
			}
		}

		return parentRecords;
	}

	public void indexDocuments(Collection<SolrInputDocument> docsForAtomicUpdates) {		
		if (!docsForAtomicUpdates.isEmpty()) {
			try {
				this.sServer.add(docsForAtomicUpdates); // Add the collection of documents to Solr
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear(); // Clear to save memory
				docsForAtomicUpdates = null; // Set to null to save memory
			}
		}
	}


}
