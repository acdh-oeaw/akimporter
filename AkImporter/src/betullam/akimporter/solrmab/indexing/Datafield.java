package betullam.akimporter.solrmab.indexing;

import java.util.List;

public class Datafield {

	private String tag;
	private String ind1;
	private String ind2;
	private List<Mabfield> mabfields;

	public Datafield() {}


	public String getTag() {
		return this.tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
	
	public String getInd1() {
		return this.ind1;
	}

	public void setInd1(String ind1) {
		this.ind1 = ind1;
	}
	
	public String getInd2() {
		return this.ind2;
	}

	public void setInd2(String ind2) {
		this.ind2 = ind2;
	}

	public List<Mabfield> getSubfields() {
		return this.mabfields;
	}

	public void setSubfields(List<Mabfield> mabfields) {
		this.mabfields = mabfields;
	}
	
}
