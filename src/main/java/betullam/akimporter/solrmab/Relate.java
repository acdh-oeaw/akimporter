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
package main.java.betullam.akimporter.solrmab;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.relations.ChildsToParentsFromChilds;
import main.java.betullam.akimporter.solrmab.relations.ChildsToParentsFromParents;
import main.java.betullam.akimporter.solrmab.relations.Generic;
import main.java.betullam.akimporter.solrmab.relations.ParentToChilds;
import main.java.betullam.akimporter.solrmab.relations.UnlinkChildsFromParents;

public class Relate {
	
	HttpSolrServer solrServer = null;
	String timeStamp = null;
	private AkImporterHelper akiHelper = null;
	boolean optimize = false;
	boolean print = true;
	boolean isRelateSuccessful = false;
	
	/**
	 * Constructor for starting the relate process between parent records and child records.
	 * 
	 * @param solrServer	The Solr server where the records are stored.
	 * @param timeStamp		Timestamp of moment the import process started.
	 * @param optimize		True if the Solr server core should be optimized after the process finished.
	 * @param print			True if status messages should be printed to console.
	 */
	public Relate(HttpSolrServer solrServer, String timeStamp, boolean optimize, boolean print) {
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
		this.optimize = optimize;
		this.print = print;
		akiHelper = new AkImporterHelper(solrServer);
		this.relate();
	}

	/**
	 * Handling the relation process between parent records and child records.
	 */
	private void relate() {
		//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//
		//+++++++++++++++++++++++++++++++++++ RELATE VOLUMES TO PARENTS ++++++++++++++++++++++++++++++++++//
		//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++//


		// 1. Linking parents to their childs:
		ParentToChilds ptc = new ParentToChilds(this.solrServer, this.timeStamp, this.print);
		ptc.addParentsToChilds();
		this.akiHelper.print(this.print, "\n");


		// 2. Remove all childs from parents:
		UnlinkChildsFromParents ucfp = new UnlinkChildsFromParents(this.solrServer, this.timeStamp, this.print);
		ucfp.unlinkChildsFromParents();
		this.akiHelper.print(this.print, "\n");


		// 3. Relink childs to parents from all currently indexed child records:
		ChildsToParentsFromChilds ctpfc = new ChildsToParentsFromChilds(this.solrServer, this.timeStamp, this.print);
		ctpfc.addChildsToParentsFromChilds();
		this.akiHelper.print(this.print, "\n");


		// 4. Relink childs to parents from all currently indexed parent records:
		ChildsToParentsFromParents ctpfp = new ChildsToParentsFromParents(this.solrServer, this.timeStamp, this.print);
		ctpfp.addChildsToParentsFromParents();
		this.akiHelper.print(this.print, "\n");


		// 5. Generic linking:
		Generic gen = null;
		gen = new Generic(this.solrServer, "otherEdition", this.timeStamp, this.print);
		gen.addGenericLink();
		this.akiHelper.print(this.print, "\n");
		gen = new Generic(this.solrServer, "attachment", this.timeStamp, this.print);
		gen.addGenericLink();
		this.akiHelper.print(this.print, "\n");
		gen = new Generic(this.solrServer, "attachementTo", this.timeStamp, this.print);
		gen.addGenericLink();
		this.akiHelper.print(this.print, "\n");
		


		if (optimize) {
			this.akiHelper.print(this.print, "Start optimizing Solr index. This could take a while. Please wait ...");
			this.akiHelper.solrOptimize();
		}
		
		isRelateSuccessful = true;
	}
	
	/**
	 * Check if the relating process between parent records and child records was successful.
	 * 
	 * @return	True if the relating process was successful.
	 */
	public boolean isRelateSuccessful() {
		return isRelateSuccessful;
	}
	
}
