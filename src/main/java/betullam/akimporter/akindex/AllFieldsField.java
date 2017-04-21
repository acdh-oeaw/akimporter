package main.java.betullam.akimporter.akindex;

import java.util.List;
import java.util.TreeMap;

public class AllFieldsField {
	
	private String tag = null;
	
	//																		  ind1            ind2    Subf
	private TreeMap<String, TreeMap<String, List<String>>> ind1 = new TreeMap<String, TreeMap<String, List<String>>>();
	
	//																		  ind2            ind1    Subf
	private TreeMap<String, TreeMap<String, List<String>>> ind2 = new TreeMap<String, TreeMap<String, List<String>>>();
	

	
	public AllFieldsField() {}

	public AllFieldsField (String tag, TreeMap<String, TreeMap<String, List<String>>> ind1, TreeMap<String, TreeMap<String, List<String>>> ind2) {
		this.tag = tag;
		this.ind1 = ind1;
		this.ind2 = ind2;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public TreeMap<String,TreeMap<String,List<String>>> getInd1() {
		return ind1;
	}

	public void setInd1(TreeMap<String, TreeMap<String, List<String>>> ind1) {
		this.ind1 = ind1;
	}

	public TreeMap<String,TreeMap<String,List<String>>> getInd2() {
		return ind2;
	}

	public void setInd2(TreeMap<String, TreeMap<String, List<String>>> ind2) {
		this.ind2 = ind2;
	}

	@Override
	public String toString() {
		return "AllFieldsField [tag=" + tag + ", ind1=" + ind1 + ", ind2=" + ind2 + "]";
	}
}