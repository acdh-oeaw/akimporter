package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

/**
 * Unlink child records (= volumes) from their parents if they are marked as deleted.
 * @author mbirkner
 *
 */
public class RelinkChildRecords {

	SolrServer sServer;
	String timeStamp;	
	SolrDocumentList childsFormCurrentIndexProcess;


	// Field name variables
	String fnChildSys = null;
	String fnChildAc = null;
	String fnChildTitle = null;
	String fnChildVolNo = null;
	String fnChildVolNoSort = null;
	String fnChildEdition = null;
	String fnChildPublishDate = null;



	public RelinkChildRecords(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
	}

	/* ############################################################################################# */
	/* ########################################## GENERAL ########################################## */
	/* ############################################################################################# */

	private SolrDocumentList getChildsFromCurrentIndexProcess() {

		// Set up variable
		SolrDocumentList resultChilds = null;

		// New Solr query
		SolrQuery queryChilds = new SolrQuery();

		// Define a query for getting all documents. We get the child records with a filter query because of performance (see below)
		queryChilds.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		queryChilds.setFilterQueries("parentAC_str:* || parentSeriesAC_str:*", "indexTimestamp_str:"+timeStamp);

		// Set fields that should be given back from the query
		queryChilds.setFields("id", "sysNo_str", "parentSYS_str", "parentAC_str", "parentSeriesAC_str");

		// Initialize Variable for query response:
		QueryResponse responseDeletedRecords = null;

		try {
			// Execute query
			responseDeletedRecords = sServer.query(queryChilds);

			// Get document-list from query result
			resultChilds = responseDeletedRecords.getResults();

			// Set null if no records were found so that we can return null
			if (resultChilds.getNumFound() <= 0) {
				resultChilds = null;
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return resultChilds;
	}

	
	private Set<String> getParentAcs() {
		Set<String> parentAcs = new HashSet<String>();
		for (SolrDocument child : getChildsFromCurrentIndexProcess()) {
			String parentAc = getParentAc(child);
			if (parentAc != null && !parentAc.isEmpty()) {
				parentAcs.add(parentAc);
			}
		}
		return parentAcs;
	}


	private String getChildRecordType(SolrDocument record) {
		String childRecordType = null;

		if (record.getFieldValue("parentAC_str") != null) {
			childRecordType = "multivolume";
		} else if (record.getFieldValue("parentSeriesAC_str") != null) {
			childRecordType = "serialvolume";
		}

		return childRecordType;
	}


	private String getParentAc(SolrDocument childRecord) {
		String parentAc = null;
		String childRecordType = getChildRecordType(childRecord);
		if (childRecordType.equals("multivolume")) {
			parentAc = childRecord.getFieldValue("parentAC_str").toString();
		} else if (childRecordType.equals("serialvolume")) {
			parentAc = childRecord.getFieldValue("parentSeriesAC_str").toString();
		}
		return parentAc;
	}
	

	private SolrDocument getParentRecord(String parentAc) {
		SolrDocument parentRecord = null;
		SolrQuery queryParent = new SolrQuery(); // New Solr query
		queryParent.setQuery("acNo_str:"+parentAc); // Define a query
		queryParent.setFields("id", "title"); // Set fields that should be given back from the query
		try {
			parentRecord = sServer.query(queryParent).getResults().get(0); // Execute query and get result
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return parentRecord;
	}

	private void setFieldNames(String recordType) {
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
	
	
	

	/* ########################################################################################################## */
	/* ########################################## LINK PARENT TO CHILD ########################################## */
	/* ########################################################################################################## */

	public void linkParentsToChilds() {

		Collection<SolrInputDocument> parentsToChilds = new ArrayList<SolrInputDocument>();
		SolrDocumentList childsFormCurrentIndexProcess = getChildsFromCurrentIndexProcess();

		for (SolrDocument childRecord : childsFormCurrentIndexProcess) {

			String childRecordId = (childRecord.getFieldValue("id") != null) ? childRecord.getFieldValue("id").toString() : null;
			String parentAc = getParentAc(childRecord);

			SolrDocument parentRecord = getParentRecord(parentAc);

			String parentRecordSys = (parentRecord.getFieldValue("id") != null) ? parentRecord.getFieldValue("id").toString() : "0";
			String parentRecordTitle = (parentRecord.getFieldValue("title") != null) ? parentRecord.getFieldValue("title").toString() : "0";

			// Prepare MU record for atomic updates:
			SolrInputDocument parentToChildDoc = null;
			parentToChildDoc = new SolrInputDocument();
			parentToChildDoc.setField("id", childRecordId);

			// Add values to MU record with atomic update:
			Map<String, String> mapMhSYS = new HashMap<String, String>();
			mapMhSYS.put("set", parentRecordSys);
			parentToChildDoc.setField("parentSYS_str", mapMhSYS);

			Map<String, String> mapMhTitle = new HashMap<String, String>();
			mapMhTitle.put("set", parentRecordTitle);
			parentToChildDoc.setField("parentTitle_str", mapMhTitle);

			// Add all values of MU child record to MH parent record:
			parentsToChilds.add(parentToChildDoc);
		}

		// Index the documents:
		if (!parentsToChilds.isEmpty()) {
			try {
				this.sServer.add(parentsToChilds); // Add the collection of documents to Solr
				this.sServer.commit(); // Commit the changes !!! IMPORTANT !!!
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				parentsToChilds = null;
			}
		}
	}

	
	

	/* ############################################################################################################### */
	/* ########################################## UNLINK CHILDS FROM PARENT ########################################## */
	/* ############################################################################################################### */
	
	private SolrInputDocument getParentRecordFromWhichToUnlink(String parentAc, String recordType) {

		SolrDocument parentRecord = getParentRecord(parentAc);
		String parentSys = (parentRecord.getFieldValue("id") != null) ? parentRecord.getFieldValue("id").toString() : "0";

		// Set field names (for serial volume or multi-volume-work volume)
		setFieldNames(recordType);

		// Prepare parent record for atomic updates:
		SolrInputDocument deleteChild = null;
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

		return deleteChild;
	}


	public void unlinkChildsFromParents() {

		this.childsFormCurrentIndexProcess = getChildsFromCurrentIndexProcess();

		Collection<SolrInputDocument> unlinkParentRecords = new ArrayList<SolrInputDocument>();

		for (SolrDocument childFromCurrentIndexProcess : childsFormCurrentIndexProcess) {

			// Get child record type
			String childRecordType = getChildRecordType(childFromCurrentIndexProcess);

			// Get parent AC no.
			String parentAc = getParentAc(childFromCurrentIndexProcess);

			// Unlink all childs from the parent record			
			unlinkParentRecords.add(getParentRecordFromWhichToUnlink(parentAc, childRecordType));
		}

		// Index the documents:
		if (!unlinkParentRecords.isEmpty()) {
			try {
				this.sServer.add(unlinkParentRecords); // Add the collection of documents to Solr
				this.sServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				unlinkParentRecords = null;
			}
		}
	}



	/* ####################################################################################################################### */
	/* ########################################## LINK NON DELETED CHILDS TO PARENT ########################################## */
	/* ####################################################################################################################### */

	private SolrDocumentList getNonDeletedChildsByParentAc(String parentAc) {

		SolrDocumentList nonDeletedChildRecords = null;

		// New Solr query
		SolrQuery querynonDeletedChilds = new SolrQuery();

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


	public void linkChildsToParents() {
		Collection<SolrInputDocument> linkedChilds = new ArrayList<SolrInputDocument>();

		for (String parentAc : getParentAcs()) {

			// Query all childs from a parent except the deleted ones
			SolrDocumentList nonDeletedChilds = getNonDeletedChildsByParentAc(parentAc);

			// Add all non deleted childs to it's parents:
			for (SolrDocument nonDeletedChild : nonDeletedChilds) {

				// Get record type
				String recordType = getChildRecordType(nonDeletedChild);

				// Set field names (for serial volume or multi-volume-work volume)
				setFieldNames(recordType);

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

					// Prepare parent record for atomic updates:
					SolrInputDocument linkedChild = null;
					linkedChild = new SolrInputDocument();
					linkedChild.setField("id", parentSys);

					// Set values for atomic update of MH record:
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

					linkedChilds.add(linkedChild);
				}
			}
		}

		// Index the documents:
		if (!linkedChilds.isEmpty()) {
			try {
				this.sServer.add(linkedChilds); // Add the collection of documents to Solr
				this.sServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				linkedChilds = null;
			}
		}
	}





	





}
