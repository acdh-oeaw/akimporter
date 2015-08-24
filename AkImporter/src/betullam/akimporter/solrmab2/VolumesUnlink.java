package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class VolumesUnlink {

	SolrServer sServer;
	String timeStamp;
	Collection<SolrInputDocument> deletedAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	//Collection<SolrInputDocument> existingAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	//Collection<SolrInputDocument> mhAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

	public VolumesUnlink(SolrServer sServer, String timeStamp) {
		this.sServer = sServer;
		this.timeStamp = timeStamp;
	}


	public void unlink() {

		
		// New Solr query
		SolrQuery queryDeletedRecords = new SolrQuery();

		// Define a query for getting all documents. We get the deleted records with a filter query because of performance (see below)
		queryDeletedRecords.setQuery("*:*");

		// Filter all deleted records that were indexed with the current import process and that are child volumes (because we need to get the parent records to be able to unlink them from there).
		queryDeletedRecords.setFilterQueries("parentAC_str:*", "deleted_str:Y", "indexTimestamp_str:"+timeStamp);

		// Set fields that should be given back from the query
		queryDeletedRecords.setFields("sysNo_str", "parentAC_str");

		// Initialize Variable for query response:
		QueryResponse responseDeletedRecords = null;


		try {
			// Execute query
			responseDeletedRecords = sServer.query(queryDeletedRecords);

			// Get document-list from query result
			SolrDocumentList resultDeletedRecords = responseDeletedRecords.getResults();
			
			if (resultDeletedRecords != null && resultDeletedRecords.getNumFound() > 0) {

				
				
				for (SolrDocument deletedDoc : resultDeletedRecords) {

					String sysNo = (deletedDoc.getFieldValue("sysNo_str") != null) ? deletedDoc.getFieldValue("sysNo_str").toString() : null;
					String parentRecordAC = (deletedDoc.getFieldValue("parentAC_str") != null) ? deletedDoc.getFieldValue("parentAC_str").toString() : null;


					// Query the parent record and get the index of the child SYS no. in the "childSYS_str_mv". If we have the index, we are able to
					// remove the other values in solr fields like "childTitle_str_mv", "childVolumeNo_str_mv", etc.

					// New Solr query
					SolrQuery queryParentRecord = new SolrQuery();

					// Define the query parameters
					queryParentRecord.setQuery("acNo_str:"+parentRecordAC);

					// Set fields that should be given back from the query
					queryParentRecord.setFields(
							"sysNo_str",
							"parentSYS_str",
							"serialvolumeSYS_str_mv",
							"serialvolumeAC_str_mv",
							"serialvolumeTitle_str_mv",
							"serialvolumeVolumeNo_str_mv",
							"serialvolumeVolumeNoSort_str_mv",
							"serialvolumeEdition_str_mv",
							"serialvolumePublishDate_str_mv",
							"childSYS_str_mv",
							"childAC_str_mv",
							"childTitle_str_mv",
							"childVolumeNo_str_mv",
							"childVolumeNoSort_str_mv",
							"childEdition_str_mv",
							"childPublishDate_str_mv"
							);


					System.out.println("###### Unlink from: " + parentRecordAC);
					
					try {
						// Get query result
						SolrDocumentList resultParentRecord = sServer.query(queryParentRecord).getResults();

						for (SolrDocument parentRecord : resultParentRecord) {

							if (parentRecord.getFieldValue("sysNo_str") == null) {
								System.out.println("###### sysNo_str of Parent Record is NULL");
							}
							
							
							// Set up some variables
							String parentRecordSYS = parentRecord.getFieldValue("sysNo_str").toString();
							List<String> lstChildSysNos = new ArrayList<String>();
							List<String> lstChildAcNos = new ArrayList<String>();
							List<String> lstChildTitles = new ArrayList<String>();
							List<String> lstChildVolumeNos = new ArrayList<String>();
							List<String> lstChildVolumeNosSort = new ArrayList<String>();
							List<String> lstChildEditions = new ArrayList<String>();
							List<String> lstChildPublishDates = new ArrayList<String>();

							// Field name variables
							String fnChildSys = null;
							String fnChildAc = null;
							String fnChildTitle = null;
							String fnChildVolNo = null;
							String fnChildVolNoSort = null;
							String fnChildEdition = null;
							String fnChildPublishDate = null;



							// Check if it is a serial volume or a multivolume-work volume and set the solr field names accordingly:
							if (parentRecord.getFieldValues("serialvolumeSYS_str_mv") != null && !parentRecord.getFieldValues("serialvolumeSYS_str_mv").isEmpty()) { // serial volume								
								fnChildSys = "serialvolumeSYS_str_mv";
								fnChildAc = "serialvolumeAC_str_mv";
								fnChildTitle = "serialvolumeTitle_str_mv";
								fnChildVolNo = "serialvolumeVolumeNo_str_mv";
								fnChildVolNoSort = "serialvolumeVolumeNoSort_str_mv";
								fnChildEdition = "serialvolumeEdition_str_mv";
								fnChildPublishDate = "serialvolumePublishDate_str_mv";
							} else if (parentRecord.getFieldValues("childSYS_str_mv") != null && !parentRecord.getFieldValues("childSYS_str_mv").isEmpty()) { // multivolume-work volume
								fnChildSys = "childSYS_str_mv";
								fnChildAc = "childAC_str_mv";
								fnChildTitle = "childTitle_str_mv";
								fnChildVolNo = "childVolumeNo_str_mv";
								fnChildVolNoSort = "childVolumeNoSort_str_mv";
								fnChildEdition = "childEdition_str_mv";
								fnChildPublishDate = "childPublishDate_str_mv";
							}


							//##################################################################################
							// TODO: 
							// 1. Get the existing child volumes as below
							// 2. Delete all existing child volumes like further below
							// 3. Re-index the existing child volumes WITHOUT the ones that should be deleted!
							//##################################################################################

							
							// Get info about existing child volumes
							lstChildSysNos = Arrays.asList(parentRecord.getFieldValues(fnChildSys).toArray(new String[0]));
							lstChildAcNos = Arrays.asList(parentRecord.getFieldValues(fnChildAc).toArray(new String[0]));
							lstChildTitles = Arrays.asList(parentRecord.getFieldValues(fnChildTitle).toArray(new String[0]));
							lstChildVolumeNos = Arrays.asList(parentRecord.getFieldValues(fnChildVolNo).toArray(new String[0]));
							lstChildVolumeNosSort = Arrays.asList(parentRecord.getFieldValues(fnChildVolNoSort).toArray(new String[0]));
							lstChildEditions = Arrays.asList(parentRecord.getFieldValues(fnChildEdition).toArray(new String[0]));
							lstChildPublishDates = Arrays.asList(parentRecord.getFieldValues(fnChildPublishDate).toArray(new String[0]));

							
							
							if (lstChildSysNos != null && !lstChildSysNos.isEmpty()) {

								// Get the index of the child volume we need to delete in the multivalued filed:
								int indexDelete = lstChildSysNos.indexOf(sysNo);
								System.out.println("###### Index to delete: " + indexDelete + " - " + lstChildTitles.get(indexDelete));
								
								// Prepare parent record for atomic updates:
								SolrInputDocument deleteFields = null;
								deleteFields = new SolrInputDocument();
								deleteFields.setField("id", parentRecordSYS);
								SolrInputDocument unlinkDeletedDoc = null;
								unlinkDeletedDoc = new SolrInputDocument();
								unlinkDeletedDoc.setField("id", parentRecordSYS);
								
								// First remove all child records:
								Map<String, String> mapRemSYS = new HashMap<String, String>();
								mapRemSYS.put("set", null);
								deleteFields.setField(fnChildSys, mapRemSYS);
								
								Map<String, String> mapRemAC = new HashMap<String, String>();
								mapRemAC.put("set", null);
								deleteFields.setField(fnChildAc, mapRemAC);

								Map<String, String> mapRemTitle = new HashMap<String, String>();
								mapRemTitle.put("set", null);
								deleteFields.setField(fnChildTitle, mapRemTitle);

								Map<String, String> mapRemVolumeNo = new HashMap<String, String>();
								mapRemVolumeNo.put("set", null);
								deleteFields.setField(fnChildVolNo, mapRemVolumeNo);

								Map<String, String> mapRemVolumeNoSort = new HashMap<String, String>();
								mapRemVolumeNoSort.put("set", null);
								deleteFields.setField(fnChildVolNoSort, mapRemVolumeNoSort);

								Map<String, String> mapRemEdition = new HashMap<String, String>();
								mapRemEdition.put("set", null);
								deleteFields.setField(fnChildEdition, mapRemEdition);

								Map<String, String> mapRemPublishDate = new HashMap<String, String>();
								mapRemPublishDate.put("set", null);
								deleteFields.setField(fnChildPublishDate, mapRemPublishDate);
																
								
								// Set values for atomic update of MH record:
								Map<String, String> mapMuSYS = new HashMap<String, String>();
								mapMuSYS.put("add", "sys");
								unlinkDeletedDoc.setField(fnChildSys, mapMuSYS);

								Map<String, String> mapMuAC = new HashMap<String, String>();
								mapMuAC.put("add", "ac");
								unlinkDeletedDoc.setField(fnChildAc, mapMuAC);

								Map<String, String> mapMuTitle = new HashMap<String, String>();
								mapMuTitle.put("add", "title");
								unlinkDeletedDoc.setField(fnChildTitle, mapMuTitle);

								Map<String, String> mapMuVolumeNo = new HashMap<String, String>();
								mapMuVolumeNo.put("add", "volno");
								unlinkDeletedDoc.setField(fnChildVolNo, mapMuVolumeNo);

								Map<String, String> mapMuVolumeNoSort = new HashMap<String, String>();
								mapMuVolumeNoSort.put("add", "volnosort");
								unlinkDeletedDoc.setField(fnChildVolNoSort, mapMuVolumeNoSort);

								Map<String, String> mapMuEdition = new HashMap<String, String>();
								mapMuEdition.put("add", "childed");
								unlinkDeletedDoc.setField(fnChildEdition, mapMuEdition);

								Map<String, String> mapMuPublishDate = new HashMap<String, String>();
								mapMuPublishDate.put("add", "pubdate");
								unlinkDeletedDoc.setField(fnChildPublishDate, mapMuPublishDate);
								
								// Add record to a record collection for updating solr:
								deletedAtomicUpdateDocs.add(deleteFields);
								deletedAtomicUpdateDocs.add(unlinkDeletedDoc);
							
								
								/*
								// Now, index the child volume fields to the parent again, but without the deleted volume.
								for (String childSysNo : lstChildSysNos) {

									int currentIndex = lstChildSysNos.indexOf(childSysNo);
									
									if (currentIndex != indexDelete) {

										
										String childSys = lstChildSysNos.get(currentIndex);
										String childAc = lstChildAcNos.get(currentIndex);
										String childTitle = lstChildTitles.get(currentIndex);
										String childVolumeNo = lstChildVolumeNos.get(currentIndex);
										String childVolumeNoSort = lstChildVolumeNosSort.get(currentIndex);
										String childEdition = lstChildEditions.get(currentIndex);
										String childPublishDate = lstChildPublishDates.get(currentIndex);

										System.out.println("Add: " + childTitle);
										
										
										
										// Prepare parent record for atomic updates:
										SolrInputDocument unlinkDeletedDoc = null;
										unlinkDeletedDoc = new SolrInputDocument();
										unlinkDeletedDoc.setField("id", parentRecordSYS);
										
										// Set values for atomic update of MH record:
										Map<String, String> mapMuSYS = new HashMap<String, String>();
										mapMuSYS.put("add", childSys);
										unlinkDeletedDoc.setField(fnChildSys, mapMuSYS);

										Map<String, String> mapMuAC = new HashMap<String, String>();
										mapMuAC.put("add", childAc);
										unlinkDeletedDoc.setField(fnChildAc, mapMuAC);

										Map<String, String> mapMuTitle = new HashMap<String, String>();
										mapMuTitle.put("add", childTitle);
										unlinkDeletedDoc.setField(fnChildTitle, mapMuTitle);

										Map<String, String> mapMuVolumeNo = new HashMap<String, String>();
										mapMuVolumeNo.put("add", childVolumeNo);
										unlinkDeletedDoc.setField(fnChildVolNo, mapMuVolumeNo);

										Map<String, String> mapMuVolumeNoSort = new HashMap<String, String>();
										mapMuVolumeNoSort.put("add", childVolumeNoSort);
										unlinkDeletedDoc.setField(fnChildVolNoSort, mapMuVolumeNoSort);

										Map<String, String> mapMuEdition = new HashMap<String, String>();
										mapMuEdition.put("add", childEdition);
										unlinkDeletedDoc.setField(fnChildEdition, mapMuEdition);

										Map<String, String> mapMuPublishDate = new HashMap<String, String>();
										mapMuPublishDate.put("add", childPublishDate);
										unlinkDeletedDoc.setField(fnChildPublishDate, mapMuPublishDate);
										
										// Add record to a record collection for updating solr:
										deletedAtomicUpdateDocs.add(unlinkDeletedDoc);
									}
								}
								*/
								

							}
						}
					} catch (SolrServerException e) {
						e.printStackTrace();
					}
				}
			}



		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		
		// Index the documents:
		if (!deletedAtomicUpdateDocs.isEmpty()) {
			try {
				this.sServer.add(deletedAtomicUpdateDocs); // Add the collection of documents to Solr
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				deletedAtomicUpdateDocs = null;
			}
		}
		
		
	}
}
