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

public class MuVolumeToParent {

	Collection<SolrInputDocument> muAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
	Collection<SolrInputDocument> mhAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

	public void addMuRecords(SolrServer sServer) {

		// New Solr query
		SolrQuery queryMUs = new SolrQuery();

		// TODO: iterate over all of the rows. Default ist 10 rows!
		//queryMUs.setRows(313); 

		// Query all MU-fields
		queryMUs.setQuery("satztyp_str:MU");


		// Set fields that should be given back from the query
		queryMUs.setFields("id", "title", "acNo_str", "parentSYS_str", "parentAC_str", "volumeNo_str", "volumeNoSort_str", "publishDate", "edition");
		QueryResponse responseMUs = null;


		try {
			// Execute query
			responseMUs = sServer.query(queryMUs);

			// Get query time and show it
			//System.out.println("QTime: " + responseMUs.getQTime());
			//System.out.println("Elapsed Time: " + responseMUs.getElapsedTime());

			// Get document-list from query result
			SolrDocumentList resultDocList = responseMUs.getResults();

			// Show how many documents were found
			long noOfMuRecords = resultDocList.getNumFound();
			System.out.println("No. of MU records found: " + noOfMuRecords);

			// If there are some records, go on. If not, do nothing.
			if (resultDocList != null && noOfMuRecords > 0) {

				System.out.println("resultDocList size: " + resultDocList.size());

				// Add counter so that we can output the no. of documents that are already processed:
				int counter = 0;

				for (SolrDocument doc : resultDocList) {

					counter = counter + 1;
					System.out.println("Processing " + counter + " from " + noOfMuRecords + " MU records");
					String muParentSYS = (doc.getFieldValue("parentSYS_str") != null) ? doc.getFieldValue("parentSYS_str").toString() : null;

					// If we already have a parentSYS_str, we already linked this MU record to it's parent MH record at some time before.
					// So we have to skip the linking process. If not, we will have the same value a second time in all the
					// multivalued fields.
					// So ONLY LINK IF muParentSYS is NULL!
					if (muParentSYS == null) {

						// Variables for atomic updates of MH and MU records:
						String muAC = (doc.getFieldValue("acNo_str") != null) ? doc.getFieldValue("acNo_str").toString() : "0";
						String muSYS = (doc.getFieldValue("id") != null) ? doc.getFieldValue("id").toString() : "0";
						String muParentAC = (doc.getFieldValue("parentAC_str") != null) ? doc.getFieldValue("parentAC_str").toString() : null;
						String muTitle = (doc.getFieldValue("title") != null) ? doc.getFieldValue("title").toString() : "0";
						String muVolumeNo = (doc.getFieldValue("volumeNo_str") != null) ? doc.getFieldValue("volumeNo_str").toString() : "0";
						String muVolumeNoSort = (doc.getFieldValue("volumeNoSort_str") != null) ? doc.getFieldValue("volumeNoSort_str").toString() : "0";
						String muEdition = (doc.getFieldValue("edition") != null) ? doc.getFieldValue("edition").toString() : "0";
						String muPublishDate = (doc.getFieldValue("publishDate") != null) ? doc.getFieldValue("publishDate").toString().replace("[", "").replace("]", "") : "0";						

						// First add data (SYS-No and title) from MH record to current MU record:
						SolrQuery queryMH = new SolrQuery(); // Query MH record of current MU record
						queryMH.setQuery("acNo_str:" + muParentAC); // Query all MU-fields
						queryMH.setFields("id", "title"); // Set fields that should be given back from the query
						QueryResponse responseMH = sServer.query(queryMH); // Execute query
						SolrDocumentList resultListMH = responseMH.getResults();

						if (!resultListMH.isEmpty() && resultListMH != null && resultListMH.getNumFound() > 0) { // Parent record exists
							SolrDocument resultDocMH = resultListMH.get(0); // Get first document from query result (there should be only one!)
							String mhSYS = (resultDocMH.getFieldValue("id") != null) ? resultDocMH.getFieldValue("id").toString() : "0";
							String mhTitle = (resultDocMH.getFieldValue("title") != null) ? resultDocMH.getFieldValue("title").toString() : "0";


							// Prepare MU record for atomic updates:
							SolrInputDocument muAtomicUpdateDoc = null;
							muAtomicUpdateDoc = new SolrInputDocument();
							muAtomicUpdateDoc.addField("id", muSYS);

							// Add values for atomic update of MU record:
							Map<String, String> mapMhSYS = new HashMap<String, String>();
							mapMhSYS.put("add", mhSYS);
							muAtomicUpdateDoc.setField("parentSYS_str", mapMhSYS);

							Map<String, String> mapMhTitle = new HashMap<String, String>();
							mapMhTitle.put("add", mhTitle);
							muAtomicUpdateDoc.setField("parentTitle_str", mapMhTitle);

							// Add all values of MU child record to MH parent record:
							muAtomicUpdateDocs.add(muAtomicUpdateDoc);


							// Prepare MH record for atomic updates:
							SolrInputDocument mhAtomicUpdateDoc = null;
							mhAtomicUpdateDoc = new SolrInputDocument();
							mhAtomicUpdateDoc.addField("id", mhSYS);

							// Add values for atomic update of MH record:
							Map<String, String> mapMuSYS = new HashMap<String, String>();
							mapMuSYS.put("add", muSYS);
							mhAtomicUpdateDoc.setField("childSYS_str_mv", mapMuSYS);

							Map<String, String> mapMuAC = new HashMap<String, String>();
							mapMuAC.put("add", muAC);
							mhAtomicUpdateDoc.setField("childAC_str_mv", mapMuAC);

							Map<String, String> mapMuTitle = new HashMap<String, String>();
							mapMuTitle.put("add", muTitle);
							mhAtomicUpdateDoc.setField("childTitle_str_mv", mapMuTitle);

							Map<String, String> mapMuVolumeNo = new HashMap<String, String>();
							mapMuVolumeNo.put("add", muVolumeNo);
							mhAtomicUpdateDoc.setField("childVolumeNo_str_mv", mapMuVolumeNo);

							Map<String, String> mapMuVolumeNoSort = new HashMap<String, String>();
							mapMuVolumeNoSort.put("add", muVolumeNoSort);
							mhAtomicUpdateDoc.setField("childVolumeNoSort_str_mv", mapMuVolumeNoSort);

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

				}


				if (muAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(muAtomicUpdateDocs);
				}

				if (mhAtomicUpdateDocs.isEmpty() == false) {
					// Now add the collection of documents to Solr:
					sServer.add(mhAtomicUpdateDocs);
				}

				// Set doc-collections to null (save memory):
				muAtomicUpdateDocs = null;
				mhAtomicUpdateDocs = null;

			}
			
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 


	}



}
