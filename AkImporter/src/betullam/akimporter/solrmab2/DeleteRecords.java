package betullam.akimporter.solrmab2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

public class DeleteRecords {

	SolrServer solrServer;
	Collection<SolrInputDocument> deletedAtomicUpdateDocs = new ArrayList<SolrInputDocument>();

	public DeleteRecords(SolrServer solrServer) {
		this.solrServer = solrServer;
	}

	public void delete(List<Record> delRecords) {

		if (!delRecords.isEmpty()) {

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

					// Set all values of parent series to serial volume:
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
