package betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import betullam.akimporter.solrmab.SolrMabHelper;

public class AuthorityFlag {

	private RelationHelper relationHelper;
	private SolrMabHelper smHelper = new SolrMabHelper();
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private HttpSolrServer solrServerAuthority;
	private boolean print = false;
	private int NO_OF_ROWS = 500;
	private int INDEX_RATE = 500;
	Set<String> gndIds = new HashSet<String>();

	public AuthorityFlag(HttpSolrServer solrServerBiblio, HttpSolrServer solrServerAuthority, String timeStamp, boolean print) {
		this.solrServerAuthority = solrServerAuthority;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServerBiblio, solrServerAuthority, timeStamp);
	}

	public void setFlagOfExistance() {
		SolrDocumentList queryResults = this.relationHelper.getRecordsWithGnd(true, null);

		// Get the number of documents that were found
		long noOfDocs = queryResults.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResults != null && noOfDocs > 0) {

			// Clear query results. We don't need them anymore.
			queryResults.clear();
			queryResults = null;

			this.smHelper.print(this.print, "Getting relevant authority records ... ");

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Add a flag to authority record that tells if it is used in bibliographic records
				lastDocId = setGndNos(isFirstPage, lastDocId);
			}

			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setGndNos(isFirstPage, lastDocId);
			}

			this.smHelper.print(this.print, "Done\n");
			this.smHelper.print(this.print, "Found " + gndIds.size() + " uses of authority records in bibliograpic index.\n");

			// Add flag of existance to authority records
			addFlagToAuthorityRecord();
			
			// Delete wrong authority records (see explanation at method):
			deleteAuhtorityWithoutHeading();

			// Commit the changes
			try {
				this.solrServerAuthority.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				gndIds.clear();
				gndIds = null;
				queryResults = null;
			}			
		}
	}

	/**
	 * Set documents for atomic Solr update and index them.
	 */
	private void addFlagToAuthorityRecord() {

		int counter = 0;
		int noOfGndIds = gndIds.size();

		if (noOfGndIds > 0) {

			for (String gndId : gndIds) {
				counter = counter + 1;

				// Prepare GND record for atomic update:
				SolrInputDocument gndRecord = null;
				gndRecord = new SolrInputDocument();
				gndRecord.setField("id", gndId);

				// Set values for atomic update of parent record:
				Map<String, String> existsInBiblio = new HashMap<String, String>();
				existsInBiblio.put("set", "true");
				gndRecord.setField("existsInBiblio_str", existsInBiblio);

				this.smHelper.print(this.print, "Setting flag in authority record " + gndId + ". Processing record no " + counter  + " of " + noOfGndIds + "                          \r");

				docsForAtomicUpdates.add(gndRecord);

				// Add documents from the class variable which was set before to Solr
				if (counter % INDEX_RATE == 0) { // Every n-th record, add documents to solr
					relationHelper.indexDocuments(docsForAtomicUpdates, solrServerAuthority);
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				} else if (counter >= noOfGndIds) { // The remainding documents (if division with NO_OF_ROWS 
					relationHelper.indexDocuments(docsForAtomicUpdates, solrServerAuthority);
					docsForAtomicUpdates.clear();
					docsForAtomicUpdates = null;
					docsForAtomicUpdates = new ArrayList<SolrInputDocument>(); // Construct a new List for SolrInputDocument
				}

			}
		}
	}


	/**
	 * Deletes authority records without heading. Due to the fact that there could be wrong authority ids in bibliographic records
	 * (e. g. typo in GND ID), there could be record stubs with just 3 fields (id, existsInBiblio_str, _version_) in the index. This
	 * happens when doing atomic updates (setting flag of existence) for wrong id numbers. Conclusion: These records are wrong (they
	 * don't even have a heading which is mandatory) and should be deleted.
	 * 
	 * INFO: We *could* prevent the creation or these record stubs while doing the atomic updates, but actually it's more performant
	 * to index the stubs and then deleting them, because otherwise we would have to check for every authority id of the bibliographic
	 * records if it exists in the authority index before doing the atomic update, which would be a lot of queries and therefore slow.
	 */
	private void deleteAuhtorityWithoutHeading() {
		try {
			this.smHelper.print(this.print, "Deleting wrong authority records                          \r");
			solrServerAuthority.deleteByQuery("-heading:*");
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	/**
	 * Adding all GND-IDs of bibliographic records to a class variable to have no duplicated values because this would cause an overhead.
	 * 
	 * @param	isFirstPage		True if first page of Solr results	
	 * @param	lastDocId		Doc ID of the last processed Solr document
	 * @return	nextDocId		Doc ID with which we want to continue
	 */
	private String setGndNos(boolean isFirstPage, String lastDocId) {
		// Variable for return value:
		String returnValue = null;

		SolrDocumentList resultDocList = relationHelper.getRecordsWithGnd(isFirstPage, lastDocId);
		String newLastDocId = resultDocList.get(resultDocList.size()-1).getFieldValue("id").toString();

		for (SolrDocument recordWithAuth : resultDocList) {

			String docId = (recordWithAuth.getFieldValue("id") != null) ? recordWithAuth.getFieldValue("id").toString() : null;

			String authorGndNo = (recordWithAuth.getFieldValue("author_GndNo_str") != null) ? recordWithAuth.getFieldValue("author_GndNo_str").toString() : null;
			String author2GndNo = (recordWithAuth.getFieldValue("author2_GndNo_str") != null) ? recordWithAuth.getFieldValue("author2_GndNo_str").toString() : null;
			String[] authorAdditionalGndNos = (recordWithAuth.getFieldValues("author_additional_GndNo_str_mv") != null) ? recordWithAuth.getFieldValues("author_additional_GndNo_str_mv").toArray(new String[0]) : null;
			String authorCorporateGndNo = (recordWithAuth.getFieldValue("corporateAuthorGndNo_str") != null) ? recordWithAuth.getFieldValue("corporateAuthorGndNo_str").toString() : null;
			String[] authorCorporate2GndNos = (recordWithAuth.getFieldValues("corporateAuthor2GndNo_str_mv") != null) ? recordWithAuth.getFieldValues("corporateAuthor2GndNo_str_mv").toArray(new String[0]) : null;				
			String subjectGndNo = (recordWithAuth.getFieldValue("subjectGndNo_str") != null) ? recordWithAuth.getFieldValue("subjectGndNo_str").toString() : null;

			// Add all possible GND Numbers to a List<String> so that we can iterate over it later on
			String replaceRegex = "(\\(.*?\\))|(GKD)"; // Replace unwanted characters in GND-ID (e. g. (DE-505), GKD, etc.)
			Set<String> gndNos = new HashSet<String>();

			if (authorGndNo != null) { gndNos.add(authorGndNo.replaceAll(replaceRegex, "")); }
			if (author2GndNo != null) { gndNos.add(author2GndNo.replaceAll(replaceRegex, "")); }
			if (authorAdditionalGndNos != null) {
				for (String authorAdditionalGndNo : authorAdditionalGndNos) {
					gndNos.add(authorAdditionalGndNo.replaceAll(replaceRegex, ""));
				}
			}
			if (authorCorporateGndNo != null) {gndNos.add(authorCorporateGndNo.replaceAll(replaceRegex, "")); }
			if (authorCorporate2GndNos != null) {
				for (String authorCorporate2GndNo : authorCorporate2GndNos) {
					gndNos.add(authorCorporate2GndNo.replaceAll(replaceRegex, ""));
				}
			}
			if (subjectGndNo != null) {gndNos.add(subjectGndNo.replaceAll(replaceRegex, "")); }


			gndIds.addAll(gndNos);

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}

		}

		return returnValue;
	}

	/*
	private boolean idExistsInGnd(String gndIdFromBibRecord) {
		boolean existsInGnd = false;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of rows
		query.setRows(0);

		// Define a query for getting document by id
		query.setQuery("id:\"" + gndIdFromBibRecord + "\"");

		// Set fields that are given back from query
		query.setFields("id");

		// Set up variable
		long numFound = 0;
		try {
			// Execute query and get results
			numFound = this.solrServerAuthority.query(query).getResults().getNumFound();
			if (numFound > 0) {
				existsInGnd = true;
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return existsInGnd;
	}
	*/
}





