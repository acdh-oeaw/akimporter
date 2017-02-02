package main.java.betullam.akimporter.solrmab;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class PostProcessor {

	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private HttpSolrServer solrServerBiblio = null;
	private boolean print = false;

	public PostProcessor(HttpSolrServer solrServerBiblio, Map<Integer, PostProcess> postProcesses, boolean print, boolean optimize) {
		this.solrServerBiblio = solrServerBiblio;
		this.print = print;

		for (Entry<Integer, PostProcess> instruction : postProcesses.entrySet()) {
			PostProcess postprocess = instruction.getValue();
			String action = postprocess.getPpAction();

			if (action.equals("replace")) {
				replace(postprocess);
			}
		}

		// Add documents to Solr
		if (!docsForAtomicUpdates.isEmpty()) {
			try {
				solrServerBiblio.add(docsForAtomicUpdates);
				solrServerBiblio.commit();
				if (optimize) {
					solrServerBiblio.optimize();
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	
	public void replace(PostProcess postprocess) {	
		String ppQuery = postprocess.getPpQuery();
		String ppField = postprocess.getPpField();
		String ppValue = postprocess.getPpValue();

		// Set up variable
		SolrDocumentList queryResult = null;

		// New Solr query
		SolrQuery query = new SolrQuery();

		// Set no of result rows
		// TODO: USE PAGEING HERE!
		query.setRows(Integer.MAX_VALUE);

		// Define a query for getting all documents. We will do a filter query further down because of performance
		query.setQuery("*:*");

		// Set filter query
		query.setFilterQueries(ppQuery);

		// Set fields that should be given back from the query
		query.setFields("id");

		try {
			// Execute query and get results
			queryResult = this.solrServerBiblio.query(query).getResults();
		} catch (SolrServerException e) {
			e.printStackTrace();
		}

		long numFound = queryResult.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (queryResult != null && numFound > 0) {

			int counter = 0;

			for (SolrDocument solrDoc : queryResult) {
				String id = (solrDoc.getFieldValue("id") != null) ?  solrDoc.getFieldValue("id").toString() : null;

				if (id != null) {
					// Prepare record for atomic updates
					SolrInputDocument parentWithoutChilds = null;
					parentWithoutChilds = new SolrInputDocument();
					parentWithoutChilds.setField("id", id);

					// Set values for atomic update
					Map<String, String> ppMap = new HashMap<String, String>();
					ppMap.put("set", ppValue);
					parentWithoutChilds.setField(ppField, ppMap);

					// Add doc for atomic update to a collection of documents
					docsForAtomicUpdates.add(parentWithoutChilds);

				}

				counter = counter + 1;
				AkImporterHelper.print(this.print, "Replace all values in field " + ppField + " with value " + ppValue + ". Processing record no " + counter  + " of " + numFound + "                       \r");

			}
		}
	}
	
}
