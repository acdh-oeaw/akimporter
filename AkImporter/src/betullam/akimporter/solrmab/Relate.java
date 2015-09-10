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
		//ParentToChilds ptc = new ParentToChilds(this.solrServer, "1441408588597", this.print);
		ptc.addParentsToChilds();
		this.smHelper.print(this.print, "\n");
		
		// 2. Remove all childs from parents:
		UnlinkChildsFromParents ucfp = new UnlinkChildsFromParents(this.solrServer, this.timeStamp, this.print);
		//UnlinkChildsFromParents ucfp = new UnlinkChildsFromParents(this.solrServer, "1441408588597", this.print);
		ucfp.unlinkChildsFromParents();
		this.smHelper.print(this.print, "\n");
		
		
		// 3. Relink childs to parents from all currently indexed child records:
		ChildsToParentsFromChilds ctpfc = new ChildsToParentsFromChilds(this.solrServer, this.timeStamp, this.print);
		//ChildsToParentsFromChilds ctp = new ChildsToParentsFromChilds(this.solrServer, "1441408588597", this.print);
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
