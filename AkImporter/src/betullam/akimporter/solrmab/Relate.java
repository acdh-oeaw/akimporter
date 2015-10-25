/**
 * Relating child and parent records.
 * This is where the steps of the linking process between child and parent records is handled.
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

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import betullam.akimporter.solrmab.relations.ChildsToParentsFromChilds;
import betullam.akimporter.solrmab.relations.ChildsToParentsFromParents;
import betullam.akimporter.solrmab.relations.ParentToChilds;
import betullam.akimporter.solrmab.relations.UnlinkChildsFromParents;

public class Relate {
	
	HttpSolrServer solrServer = null;
	String timeStamp = null;
	private SolrMabHelper smHelper = null;
	boolean optimize = false;
	boolean print = true;
	boolean isRelateSuccessful = false;
	
	public Relate(HttpSolrServer solrServer, String timeStamp, boolean optimize, boolean print) {
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
		this.optimize = optimize;
		this.print = print;
		smHelper = new SolrMabHelper(solrServer);
		this.relate();
	}

	private void relate() {
		//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		//+++++++++++++++++++++++++++++++++++ RELINK VOLUMES TO PARENTS ++++++++++++++++++++++++++++++++++//
		//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		
		// 1. Linking parents to their childs:
		ParentToChilds ptc = new ParentToChilds(this.solrServer, this.timeStamp, this.print);
		ptc.addParentsToChilds();
		this.smHelper.print(this.print, "\n");
		
		// 2. Remove all childs from parents:
		UnlinkChildsFromParents ucfp = new UnlinkChildsFromParents(this.solrServer, this.timeStamp, this.print);
		ucfp.unlinkChildsFromParents();
		this.smHelper.print(this.print, "\n");
		
		
		// 3. Relink childs to parents from all currently indexed child records:
		ChildsToParentsFromChilds ctpfc = new ChildsToParentsFromChilds(this.solrServer, this.timeStamp, this.print);
		ctpfc.addChildsToParentsFromChilds();
		this.smHelper.print(this.print, "\n");
		
		// 4. Relink childs to parents from all currently indexed parent records:
		ChildsToParentsFromParents ctpfp = new ChildsToParentsFromParents(this.solrServer, this.timeStamp, this.print);
		ctpfp.addChildsToParentsFromParents();
		this.smHelper.print(this.print, "\n");

		
		if (optimize) {
			this.smHelper.print(this.print, "Start optimizing Solr index. This could take a while. Please wait ...");
			this.smHelper.solrOptimize();
		}
		
		isRelateSuccessful = true;
	}
	
	public boolean isRelateSuccessful() {
		return isRelateSuccessful;
	}
	
}
