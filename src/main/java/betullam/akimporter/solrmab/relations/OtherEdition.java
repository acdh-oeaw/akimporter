package main.java.betullam.akimporter.solrmab.relations;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class OtherEdition {

	private HttpSolrServer solrServerBiblio;
	
	public OtherEdition(HttpSolrServer solrServerBiblio) {
		this.solrServerBiblio = solrServerBiblio;
	}
	
	
	/**
	 * 1. Get all records with the field "otherEditionId_str_mv"
	 * 2. Iterate over these records and use "getOtherEditionRecord" to get the Solr-Record-ID (SYS-No.) of the record for the "other Edition"
	 * 3. Add the Solr-Record-ID (SYS-No.) to the record with atomic updates.
	 */
	
	
	/**
	 * Getting a record of the "other Edition" by its AC-No. or ZDB ID
	 * 
	 * @param id		AC-No. or ZDB ID of record
	 * @return			SolrDocument representing the record of the other Edition
	 */
	private SolrDocument getOtherEditionRecord(String id) {
		SolrDocument otherEditionRecord = null;
		SolrQuery queryOtherEdition = new SolrQuery(); // New Solr query
		queryOtherEdition.setQuery("acNo_txt:\""+id+"\" || zdbId_txt:\""+id+"\""); // Define a query
		queryOtherEdition.setFields("id", "title"); // Set fields that should be given back from the query

		try {
			SolrDocumentList resultList = this.solrServerBiblio.query(queryOtherEdition).getResults();
			if (resultList.getNumFound() > 0 && resultList != null) {
				otherEditionRecord = this.solrServerBiblio.query(queryOtherEdition).getResults().get(0);// Get Solr document (there should only be one)
			}
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		return otherEditionRecord;
	}
}
