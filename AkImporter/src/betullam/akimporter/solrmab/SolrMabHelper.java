package betullam.akimporter.solrmab;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;

public class SolrMabHelper {

	private HttpSolrServer solrServer = null;
	
	public SolrMabHelper() {}
	
	public SolrMabHelper(HttpSolrServer solrServer) {
		this.solrServer = solrServer;
		
	}
	
	public String getExecutionTime(long startTime, long endTime) {
		String executionTime = null;

		long timeElapsedMilli =  endTime - startTime;
		int seconds = (int) (timeElapsedMilli / 1000) % 60 ;
		int minutes = (int) ((timeElapsedMilli / (1000*60)) % 60);
		int hours   = (int) ((timeElapsedMilli / (1000*60*60)) % 24);

		executionTime = hours + ":" + minutes + ":" + seconds;
		return executionTime;
	}
	
	public void solrOptimize() {
		try {
			this.solrServer.optimize();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void print(boolean doPrint, String text) {
		if (doPrint) {
			System.out.print(text);
		}
	}
}
