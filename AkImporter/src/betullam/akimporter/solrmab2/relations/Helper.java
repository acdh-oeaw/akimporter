package betullam.akimporter.solrmab2.relations;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
	int NO_OF_ROWS = 1000;
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
			query.setFilterQueries("parentAC_str:* || parentSeriesAC_str:*", "indexTimestamp_str:"+this.timeStamp, "id:*");
		} else { // After the first query, we need to use ranges to get the appropriate results
			query.setStart(1);
			query.setFilterQueries("parentAC_str:* || parentSeriesAC_str:*", "indexTimestamp_str:"+this.timeStamp, "id:[" + lastDocId + " TO *]");
		}

		// Set fields that should be given back from the query
		query.setFields("id", "sysNo_str", "parentSYS_str", "parentAC_str", "parentSeriesAC_str");


		try {
			// Execute query and get results
			queryResult = sServer.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}
		return queryResult;
	}

	public Set<String> getParentAcs(SolrDocumentList childDocumentList) {
		Set<String> parentAcs = new HashSet<String>();
		for (SolrDocument child : childDocumentList) {
			String parentAc = getParentAc(child);
			if (parentAc != null && !parentAc.isEmpty()) {
				parentAcs.add(parentAc);
			}
		}
		return parentAcs;
	}


	public String getChildRecordType(SolrDocument record) {
		String childRecordType = null;

		if (record.getFieldValue("parentAC_str") != null) {
			childRecordType = "multivolume";
		} else if (record.getFieldValue("parentSeriesAC_str") != null) {
			childRecordType = "serialvolume";
		} else {
			System.err.println("Returnd fields in solr query must include the fields \"parentAC_str\" and \"parentSeriesAC_str\".");
			return null;
		}

		return childRecordType;
	}


	public String getParentAc(SolrDocument childRecord) {
		String parentAc = null;
		String childRecordType = getChildRecordType(childRecord);
		if (childRecordType.equals("multivolume")) {
			parentAc = childRecord.getFieldValue("parentAC_str").toString();
		} else if (childRecordType.equals("serialvolume")) {
			parentAc = childRecord.getFieldValue("parentSeriesAC_str").toString();
		}
		return parentAc;
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

	public void setFieldNames(String recordType) {
		// Set the solr field names accordingly:
		if (recordType.equals("serialvolume")) { // serial volume								
			fnChildSys = "serialvolumeSYS_str_mv";
			fnChildAc = "serialvolumeAC_str_mv";
			fnChildTitle = "serialvolumeTitle_str_mv";
			fnChildVolNo = "serialvolumeVolumeNo_str_mv";
			fnChildVolNoSort = "serialvolumeVolumeNoSort_str_mv";
			fnChildEdition = "serialvolumeEdition_str_mv";
			fnChildPublishDate = "serialvolumePublishDate_str_mv";
		} else if (recordType.equals("multivolume")) { // multivolume-work volume
			fnChildSys = "childSYS_str_mv";
			fnChildAc = "childAC_str_mv";
			fnChildTitle = "childTitle_str_mv";
			fnChildVolNo = "childVolumeNo_str_mv";
			fnChildVolNoSort = "childVolumeNoSort_str_mv";
			fnChildEdition = "childEdition_str_mv";
			fnChildPublishDate = "childPublishDate_str_mv";
		}
	}
	
	public Map<String, String> getFieldNames(String recordType) {
		
		setFieldNames(recordType);
		
		Map<String, String> mapOfFields = new HashMap<String, String>();

		
		mapOfFields.put("childSys", fnChildSys);
		mapOfFields.put("childAc", fnChildAc);
		mapOfFields.put("childTitle", fnChildTitle);
		mapOfFields.put("childVolNo", fnChildVolNo);
		mapOfFields.put("childVolNoSort", fnChildVolNoSort);
		mapOfFields.put("childEdition", fnChildEdition);
		mapOfFields.put("childPublishDate", fnChildPublishDate);


		return mapOfFields;
	}
	
	public void indexDocuments(Collection<SolrInputDocument> docsForAtomicUpdates) {
		if (!docsForAtomicUpdates.isEmpty()) {
			try {
				this.sServer.add(docsForAtomicUpdates); // Add the collection of documents to Solr
				//this.sServer.commit(); // Commit the changes
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates = null; // Set to null to save memory
			}
		}
	}
	
	
}
