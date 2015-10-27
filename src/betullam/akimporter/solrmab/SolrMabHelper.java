/**
 * Helper class for indexing data to Solr.
 *  
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
 * 
 * This file is part of AkImporter.
 * 
 * AkImporter is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AkImporter is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AkImporter.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Michael Birkner <michael.birkner@akwien.at>
 * @license  http://www.gnu.org/licenses/gpl-3.0.html
 * @link     http://wien.arbeiterkammer.at/service/bibliothek/
 */
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
	
	/**
	 * Get human readable execution time between two moments in time expressed in milliseconds.
	 * @param startTime		Moment of start in milliseconds
	 * @param endTime		Moment of end in milliseconds
	 * @return				String of human readable execution time.
	 */
	public String getExecutionTime(long startTime, long endTime) {
		String executionTime = null;

		long timeElapsedMilli =  endTime - startTime;
		int seconds = (int) (timeElapsedMilli / 1000) % 60 ;
		int minutes = (int) ((timeElapsedMilli / (1000*60)) % 60);
		int hours   = (int) ((timeElapsedMilli / (1000*60*60)) % 24);

		executionTime = hours + ":" + minutes + ":" + seconds;
		return executionTime;
	}
	
	/**
	 * Starts an optimize action for a Solr core.
	 */
	public void solrOptimize() {
		try {
			this.solrServer.optimize();
		} catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Prints a text message to the console if "print" is true.
	 * @param print		True if the message should be printed.
	 * @param text		The text to print to the console.
	 */
	public void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}
}
