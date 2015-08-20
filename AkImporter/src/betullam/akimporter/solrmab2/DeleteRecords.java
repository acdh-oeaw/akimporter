package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

public class DeleteRecords {

	SolrServer solrServer;
	Collection<SolrInputDocument> deletedAtomicUpdateDocs;

	public DeleteRecords(SolrServer solrServer) {
		this.solrServer = solrServer;
	}

	public void delete(List<Record> delRecords) {

		if (!delRecords.isEmpty()) {

			deletedAtomicUpdateDocs = new ArrayList<SolrInputDocument>();
			
			for (Record delRecord : delRecords) {

				String recordSYS = delRecord.getRecordSYS();

				if (!recordSYS.trim().isEmpty()) {
					// Prepare record for atomic updates:
					SolrInputDocument deletedAtomicUpdateDoc = null;
					deletedAtomicUpdateDoc = new SolrInputDocument();
					deletedAtomicUpdateDoc.setField("id", recordSYS);

					// Set "deleted_boolean" field:
					Map<String, Boolean> mapDeleted = new HashMap<String, Boolean>();
					mapDeleted.put("set", true);
					deletedAtomicUpdateDoc.setField("deleted_boolean", mapDeleted);

					
					// Update the timestamp field:
					Map<String, String> mapTimestamp = new HashMap<String, String>();
					mapTimestamp.put("set", delRecord.getIndexTimestamp());
					deletedAtomicUpdateDoc.setField("indexTimestamp_str", mapTimestamp);
					
					
					
					
					
					
					// Updating timestamp - BEGIN
					
					/* Update the timestamp field "indexTimestamp_str" from all other volumes of the same parent record and the parent record itself.
					 * This is important because we need to re-link the child records to it's parent records, and this only happens
					 * if their "indexTimestamp_str" field has the timestamp of the current import process. If they have an old timestamp
					 * because they were indexed earlier, they would not be updated. That means: The parent record would still have
					 * a link to it's child record, even when it was deleted. The result would be a dead link.
					 */
					String parentSYS = null;
					
					//System.out.println("\n----------------\nParentSYS: " + delRecord.getMabfields().toString() +"\n----------------\n");
					
					for (Mabfield mabfield : delRecord.getMabfields()) {
						parentSYS = (mabfield.getFieldname().equals("parentSYS_str")) ? parentSYS : null;
					}
					//System.out.println("\n----------------\nParentSYS: " + parentSYS +"\n----------------\n");
					// New Solr query
					SolrQuery fqUpdateTimestamp = new SolrQuery();

					// Define a query for getting all documents. We get the serial volumes with a filter query because of performance (see below)
					fqUpdateTimestamp.setQuery("parentSYS_str:"+parentSYS+" || sysNo_str:"+parentSYS);
					fqUpdateTimestamp.setFilterQueries("-sysNo_str:"+recordSYS); // Remove this current record because it's already updated
					
					// Updating timestamp - END
					
					
					
					
					
					
					
					// Add doc to collection of documents:
					deletedAtomicUpdateDocs.add(deletedAtomicUpdateDoc);
				}
			}

			if (!deletedAtomicUpdateDocs.isEmpty()) {
				try {
					solrServer.add(deletedAtomicUpdateDocs); // Add the collection of documents to Solr
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
}
