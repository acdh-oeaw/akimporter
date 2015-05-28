package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class SerialVolumeToParent {

	
	Collection<SolrInputDocument> serialVolumeAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	Collection<SolrInputDocument> parentSeriesAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	
	public void addSerialVolumes(SolrServer sServer) {

		// New Solr query
		SolrQuery querySerialVolumes = new SolrQuery();

		// TODO: iterate over all of the rows. Default ist 10 rows!
		//querySerialVolumes.setRows(313); 

		// Query all MU-fields
		querySerialVolumes.setQuery("parentSeriesAC_str:*");

		// Set fields that should be given back from the query
		querySerialVolumes.setFields("id", "title", "acNo_str", "parentSeriesSYS_str", "parentSeriesAC_str", "serialVolumeNo_str", "serialVolumeNoSort_str", "publishDate", "edition");
		QueryResponse responseSerialVolumes = null;

		try {
			// Execute query
			responseSerialVolumes = sServer.query(querySerialVolumes);

			// Get document-list from query result
			SolrDocumentList resultSerialVolumesList = responseSerialVolumes.getResults();

			// Show how many documents were found
			long noOfSerialVolumes = resultSerialVolumesList.getNumFound();
			System.out.println("No. of serial volumes found: " + noOfSerialVolumes);

			// If there are some records, go on. If not, do nothing.
			if (resultSerialVolumesList != null && noOfSerialVolumes > 0) {

				System.out.println("resultDocList size: " + resultSerialVolumesList.size());
				
				// Add counter so that we can output the no. of documents that are already processed:
				int counter = 0;
				
				for (SolrDocument doc : resultSerialVolumesList) {

					counter = counter + 1;
					System.out.println("Processing " + counter + " from " + noOfSerialVolumes + " serial volumes");
					String serialParentSYS = (doc.getFieldValue("parentSeriesSYS_str") != null) ? doc.getFieldValue("parentSeriesSYS_str").toString() : null;
				
					// If we already have a parentSeriesSYS_str, we already linked this serial volume to it's parent series record at some time before.
					// So we have to skip the linking process. If not, we will have the same value a second time in all the multivalued fields.
					// So ONLY LINK IF parentSeriesSYS_str is NULL!
					if (serialParentSYS == null) {
						
						// Variables for atomic updates of serial volume and parent series records:
						String serialvolAC = (doc.getFieldValue("acNo_str") != null) ? doc.getFieldValue("acNo_str").toString() : "0";
						String serialvolSYS = (doc.getFieldValue("id") != null) ? doc.getFieldValue("id").toString() : "0";
						String serialvolParentAC = (doc.getFieldValue("parentSeriesAC_str") != null) ? doc.getFieldValue("parentSeriesAC_str").toString() : null;
						String serialvolTitle = (doc.getFieldValue("title") != null) ? doc.getFieldValue("title").toString() : "0";
						String serialvolVolumeNo = (doc.getFieldValue("serialVolumeNo_str") != null) ? doc.getFieldValue("serialVolumeNo_str").toString() : "0";
						String serialvolVolumeNoSort = (doc.getFieldValue("serialVolumeNoSort_str") != null) ? doc.getFieldValue("serialVolumeNoSort_str").toString() : "0";
						String serialvolEdition = (doc.getFieldValue("edition") != null) ? doc.getFieldValue("edition").toString() : "0";
						String serialvolPublishDate = (doc.getFieldValue("publishDate") != null) ? doc.getFieldValue("publishDate").toString().replace("[", "").replace("]", "") : "0";
						
						// First add data (SYS-No and title) from parent series record to current serial volume record:
						SolrQuery queryParentSeries = new SolrQuery(); // Query MH record of current MU record
						queryParentSeries.setQuery("acNo_str:" + serialvolParentAC); // Query all MU-fields
						queryParentSeries.setFields("id", "title"); // Set fields that should be given back from the query
						QueryResponse responseParentSeries = sServer.query(queryParentSeries); // Execute query
						SolrDocumentList resultListParentSeries = responseParentSeries.getResults();
						
						if (!resultListParentSeries.isEmpty() && resultListParentSeries != null && resultListParentSeries.getNumFound() > 0) { // Parent record exists
							SolrDocument resultDocParentSeries = resultListParentSeries.get(0); // Get first document from query result (there should be only one!)
							String parentSeriesSYS = (resultDocParentSeries.getFieldValue("id") != null) ? resultDocParentSeries.getFieldValue("id").toString() : "0";
							String parentSeriesTitle = (resultDocParentSeries.getFieldValue("title") != null) ? resultDocParentSeries.getFieldValue("title").toString() : "0";
						
							// Prepare serial volume for atomic updates:
							SolrInputDocument serialvolAtomicUpdateDoc = null;
							serialvolAtomicUpdateDoc = new SolrInputDocument();
							serialvolAtomicUpdateDoc.addField("id", serialvolSYS);

							// Add values for atomic update of MU record:
							Map<String, String> mapParentSeriesSYS = new HashMap<String, String>();
							mapParentSeriesSYS.put("add", parentSeriesSYS);
							serialvolAtomicUpdateDoc.setField("parentSeriesSYS_str", mapParentSeriesSYS);

							Map<String, String> mapParentSeriesTitle = new HashMap<String, String>();
							mapParentSeriesTitle.put("add", parentSeriesTitle);
							serialvolAtomicUpdateDoc.setField("parentSeriesTitle_str", mapParentSeriesTitle);

							// Add all values of MU child record to MH parent record:
							serialVolumeAtomicUpdateDocs.add(serialvolAtomicUpdateDoc);
							

							// Prepare MH record for atomic updates:
							SolrInputDocument parentSeriesAtomicUpdateDoc = null;
							parentSeriesAtomicUpdateDoc = new SolrInputDocument();
							parentSeriesAtomicUpdateDoc.addField("id", parentSeriesSYS);

							// Add values for atomic update of MH record:
							Map<String, String> mapSerialVolumeSYS = new HashMap<String, String>();
							mapSerialVolumeSYS.put("add", serialvolSYS);
							parentSeriesAtomicUpdateDoc.setField("serialvolumeSYS_str_mv", mapSerialVolumeSYS);

							Map<String, String> mapSerialVolumeAC = new HashMap<String, String>();
							mapSerialVolumeAC.put("add", serialvolAC);
							parentSeriesAtomicUpdateDoc.setField("serialvolumeAC_str_mv", mapSerialVolumeAC);

							Map<String, String> mapSerialVolumeTitle = new HashMap<String, String>();
							mapSerialVolumeTitle.put("add", serialvolTitle);
							parentSeriesAtomicUpdateDoc.setField("serialvolumeTitle_str_mv", mapSerialVolumeTitle);

							Map<String, String> mapSerialVolumeVolumeNo = new HashMap<String, String>();
							mapSerialVolumeVolumeNo.put("add", serialvolVolumeNo);
							parentSeriesAtomicUpdateDoc.setField("serialvolumeVolumeNo_str_mv", mapSerialVolumeVolumeNo);

							Map<String, String> mapSerialVolumeVolumeNoSort = new HashMap<String, String>();
							mapSerialVolumeVolumeNoSort.put("add", serialvolVolumeNoSort);
							parentSeriesAtomicUpdateDoc.setField("serialvolumeVolumeNoSort_str_mv", mapSerialVolumeVolumeNoSort);

							Map<String, String> mapSerialVolumeEdition = new HashMap<String, String>();
							mapSerialVolumeEdition.put("add", serialvolEdition);
							parentSeriesAtomicUpdateDoc.setField("serialvolumeEdition_str_mv", mapSerialVolumeEdition);

							Map<String, String> mapSerialVolumePublishDate = new HashMap<String, String>();
							mapSerialVolumePublishDate.put("add", serialvolPublishDate);
							parentSeriesAtomicUpdateDoc.setField("serialvolumePublishDate_str_mv", mapSerialVolumePublishDate);

							// Add all values of MU child record to MH parent record:
							parentSeriesAtomicUpdateDocs.add(parentSeriesAtomicUpdateDoc);

							
						}
						
					}
				}
				
				if (serialVolumeAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(serialVolumeAtomicUpdateDocs);
				}

				if (parentSeriesAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(parentSeriesAtomicUpdateDocs);
				}

				// Set doc-collections to null (save memory):
				serialVolumeAtomicUpdateDocs = null;
				parentSeriesAtomicUpdateDocs = null;
			}

		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 

	}
}
