package betullam.akimporter.solrmab.relations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.SortClause;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import betullam.akimporter.solrmab.SolrMabHelper;

/**
 * A parent record may be update. Then, the existing parent record in solr will be overwritten because it will be indexed from scratch.
 * Then, the info for child records will be deleted too. If a child record of that parent is indexed too, then everything is OK because
 * this one and all other childs will be added to the parent. If no child record is indexed, the parent will remain without children. Thats
 * why we need to look if the record has children, and if not, we need to see if there are children at all. If yes, we need to link them to
 * the parent. If not, then there are no children and we can leave the parent record as it is.
 * 
 */
public class ChildsToParentsFromParents {

	// General variables
	private HttpSolrServer solrServer;
	private Collection<SolrInputDocument> docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
	private RelationHelper relationHelper;
	private SolrMabHelper smHelper = new SolrMabHelper();
	private int NO_OF_ROWS = 500;
	private Set<String> parentSYSs = new HashSet<String>();
	private boolean print = false;




	public ChildsToParentsFromParents(HttpSolrServer solrServer, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServer, timeStamp);
	}


	public void addChildsToParentsFromParents() {

		SolrDocumentList recordsWithNoChilds = relationHelper.getCurrentlyIndexedRecordsWithNoChilds(true, null);

		// Get the number of documents that were found
		long noOfDocs = recordsWithNoChilds.getNumFound();

		// If there are some records, go on. If not, do nothing.
		if (recordsWithNoChilds != null && noOfDocs > 0) {

			// Clear query results to save memory. We don't need it anymore.
			recordsWithNoChilds.clear();
			recordsWithNoChilds = null;

			// Calculate the number of solr result pages we need to iterate over
			long wholePages = (noOfDocs/NO_OF_ROWS);
			long fractionPages = (noOfDocs%NO_OF_ROWS);

			// Variable for lastDocId
			String lastDocId = null;

			
			for (long l = 0; l < wholePages; l++) {
				boolean isFirstPage = (l == 0) ? true : false;

				// Set the AC or SYS Nos from the parents to a class variable of type "Set<String>" to get a list without duplicated
				// values to avoid an overhead
				lastDocId = setParentAtomicUpdateDocs(isFirstPage, lastDocId);
				
				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection to save memory
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			
			// Add documents on the last page:
			if (fractionPages != 0) {
				boolean isFirstPage = (wholePages <= 0) ? true : false;

				// If there is no whole page but only a fraction page, the fraction page is the first page, because it's the only one
				setParentAtomicUpdateDocs(isFirstPage, lastDocId);
				
				// Add documents to Solr
				relationHelper.indexDocuments(docsForAtomicUpdates);

				// Set Collection<SolrInputDocument> to null and then to a fresh Collection to save memory
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				docsForAtomicUpdates = new ArrayList<SolrInputDocument>();
			}

			
			// Commit the changes
			try {
				this.solrServer.commit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				docsForAtomicUpdates.clear();
				docsForAtomicUpdates = null;
				parentSYSs.clear();
				parentSYSs = null;
				recordsWithNoChilds = null;
			}			
		}
	}


	// Add child records too all parents which have unlinked childs in solr
	public String setParentAtomicUpdateDocs(boolean isFirstPage, String lastDocId) {
		
		// Variable for return value:
		String returnValue = null;

		// Records without childs (these could be parents with childs that are not linked yet)
		SolrDocumentList recordsWithNoChilds = relationHelper.getCurrentlyIndexedRecordsWithNoChilds(true, null);

		int counter = 0;
		long noOfParents = recordsWithNoChilds.getNumFound();

		// Deep paging stuff (better performance)
		String newLastDocId = recordsWithNoChilds.get(recordsWithNoChilds.size()-1).getFieldValue("id").toString();
		String docId = null;

		for (SolrDocument recordWithNoChild : recordsWithNoChilds) {

			String parentSYS = (recordWithNoChild.getFieldValue("id") != null) ? recordWithNoChild.getFieldValue("id").toString() : null;

			if (parentSYS != null) {

				// Get all non deleted childs of this records
				SolrDocumentList nonDeletedChilds = getNonDeletedChildsByParentSYS(parentSYS);

				// If childs are existing, link them to it's parent
				if (nonDeletedChilds != null && nonDeletedChilds.getNumFound() > 0) {

					// Some Lists to add infos from multiple child records
					List<String> childTypes = new ArrayList<String>();
					List<String> childSYSs = new ArrayList<String>();
					List<String> childACs = new ArrayList<String>();
					List<String> childTitles = new ArrayList<String>();
					List<String> childVolumeNos = new ArrayList<String>();
					List<String> childVolumeNosSort = new ArrayList<String>();
					List<String> childEditions = new ArrayList<String>();
					List<String> childPublishDates = new ArrayList<String>();

					// Add childs to this parent
					for (SolrDocument nonDeletedChild : nonDeletedChilds) {

						// Get type of child record (multivolume, serialvolume, etc.)
						String childType = relationHelper.getChildRecordType(nonDeletedChild);

						// Get child infos
						String childSys = (nonDeletedChild.getFieldValue("sysNo_txt") != null) ? nonDeletedChild.getFieldValue("sysNo_txt").toString() : "0";
						String childAc = (nonDeletedChild.getFieldValue("acNo_txt") != null) ? nonDeletedChild.getFieldValue("acNo_txt").toString() : "0";
						String childTitle = (nonDeletedChild.getFieldValue("title") != null) ? nonDeletedChild.getFieldValue("title").toString() : "0";
						String childVolumeNo = "0";
						String childVolumeNoSort = "0";
						if (childType.equals("multivolume")) {
							childVolumeNo = (nonDeletedChild.getFieldValue("multiVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNo_str").toString() : "0";
							childVolumeNoSort = (nonDeletedChild.getFieldValue("multiVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("multiVolumeNoSort_str").toString() : "0";
						} else if (childType.equals("serialvolume")) {
							childVolumeNo = (nonDeletedChild.getFieldValue("serialVolumeNo_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNo_str").toString() : "0";
							childVolumeNoSort = (nonDeletedChild.getFieldValue("serialVolumeNoSort_str") != null) ? nonDeletedChild.getFieldValue("serialVolumeNoSort_str").toString() : "0";
						} else if (childType.equals("article")) {
							childVolumeNo = "0";
							childVolumeNoSort = "0";
						}
						String childEdition = (nonDeletedChild.getFieldValue("edition") != null) ? nonDeletedChild.getFieldValue("edition").toString() : "0";
						String childPublishDate = (nonDeletedChild.getFieldValue("publishDate") != null) ? nonDeletedChild.getFieldValue("publishDate").toString() : "0";

						// Add child infos to Lists
						childTypes.add(childType);
						childSYSs.add(childSys);
						childACs.add(childAc);
						childTitles.add(childTitle);
						childVolumeNos.add(childVolumeNo);
						childVolumeNosSort.add(childVolumeNoSort);
						childEditions.add(childEdition);
						childPublishDates.add(childPublishDate);
					}

					// Prepare parent record for atomic updates:
					SolrInputDocument linkedChild = null;
					linkedChild = new SolrInputDocument();
					linkedChild.setField("id", parentSYS);

					// Set values for atomic update of parent record:
					Map<String, List<String>> mapChildType = new HashMap<String, List<String>>();
					mapChildType.put("set", childTypes);
					linkedChild.setField("childType_str_mv", mapChildType);

					Map<String, List<String>> mapChildSYS = new HashMap<String, List<String>>();
					mapChildSYS.put("set", childSYSs);
					linkedChild.setField("childSYS_str_mv", mapChildSYS);

					Map<String, List<String>> mapChildAC = new HashMap<String, List<String>>();
					mapChildAC.put("set", childACs);
					linkedChild.setField("childAC_str_mv", mapChildAC);

					Map<String, List<String>> mapChildTitle = new HashMap<String, List<String>>();
					mapChildTitle.put("set", childTitles);
					linkedChild.setField("childTitle_str_mv", mapChildTitle);

					Map<String, List<String>> mapChildVolumeNo = new HashMap<String, List<String>>();
					mapChildVolumeNo.put("set", childVolumeNos);
					linkedChild.setField("childVolumeNo_str_mv", mapChildVolumeNo);

					Map<String, List<String>> mapChildVolumeNoSort = new HashMap<String, List<String>>();
					mapChildVolumeNoSort.put("set", childVolumeNosSort);
					linkedChild.setField("childVolumeNoSort_str_mv", mapChildVolumeNoSort);

					Map<String, List<String>> mapChildEdition = new HashMap<String, List<String>>();
					mapChildEdition.put("set", childEditions);
					linkedChild.setField("childEdition_str_mv", mapChildEdition);

					Map<String, List<String>> mapChildPublishDate = new HashMap<String, List<String>>();
					mapChildPublishDate.put("set", childPublishDates);
					linkedChild.setField("childPublishDate_str_mv", mapChildPublishDate);

					docsForAtomicUpdates.add(linkedChild);
				}
			}

			
			counter = counter + 1;
			this.smHelper.print(this.print, "Linking childs to parent from unlinked parents. Processing record no " + counter  + " of " + noOfParents + "            \r");
			this.smHelper.print(this.print, StringUtils.repeat("\b", 130) + "\r");

			docId = (recordWithNoChild.getFieldValue("id") != null) ? recordWithNoChild.getFieldValue("id").toString() : null;

			// If the last document of the solr result page is reached, build a new filter query so that we can iterate over the next result page:
			if (docId.equals(newLastDocId)) {
				returnValue = docId;
			}
		}


		return returnValue;
	}


	private SolrDocumentList getNonDeletedChildsByParentSYS(String parentSYS) {

		SolrDocumentList nonDeletedChildRecords = null;

		// New Solr query
		SolrQuery querynonDeletedChilds = new SolrQuery();

		// Add sorting
		querynonDeletedChilds.addSort(new SortClause("id", SolrQuery.ORDER.asc));

		// Set rows to a very high number so that we really get all child records
		// This should not be the bottleneck concerning performance because no parent record will have
		// such a high number of child volumes
		querynonDeletedChilds.setRows(500000);

		// Define a query for getting all documents. We get the deleted records with a filter query because of performance (see below)
		querynonDeletedChilds.setQuery("*:*");

		// Filter all records that were indexed with the current import process and that are child volumes
		// (because we need to get their parent records to be able to unlink these childs from there).
		querynonDeletedChilds.setFilterQueries("parentSYS_str_mv:"+parentSYS, "-deleted_str:Y");

		// Set fields that should be given back from the query
		querynonDeletedChilds.setFields(
				"sysNo_txt",
				"acNo_txt",
				"title",
				"multiVolumeNo_str",
				"multiVolumeNoSort_str",
				"serialVolumeNo_str",
				"serialVolumeNoSort_str",
				"edition",
				"publishDate",
				"parentSYS_str_mv",
				"parentMultiAC_str",
				"parentSeriesAC_str_mv",
				"articleParentAC_str"
				);
		try {
			nonDeletedChildRecords = solrServer.query(querynonDeletedChilds).getResults();			
		} catch (SolrServerException e) {
			nonDeletedChildRecords = null;
			e.printStackTrace();
		}

		return nonDeletedChildRecords;
	}

}
