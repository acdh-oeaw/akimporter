package betullam.akimporter.solrmab;

import java.util.List;

public class Record {

	private List<Datafield> datafields;
	private List<Controlfield> controlfields;
	private List<Mabfield> mabfields;
	private String satztyp;
	private String recordID;
	

	public Record() {}
	
	public List<Datafield> getDatafields() {
		return this.datafields;
	}

	public void setDatafields(List<Datafield> datafields) {
		this.datafields = datafields;
	}

	public List<Controlfield> getControlfields() {
		return this.controlfields;
	}

	public void setControlfields(List<Controlfield> controlfields) {
		this.controlfields = controlfields;
	}

	public List<Mabfield> getMabfields() {
		return mabfields;
	}

	public void setMabfields(List<Mabfield> mabfields) {
		this.mabfields = mabfields;
	}

	public String getSatztyp() {
		return satztyp;
	}

	public void setSatztyp(String satztyp) {
		this.satztyp = satztyp;
	}

	public String getRecordID() {
		return recordID;
	}

	public void setRecordID(String recordID) {
		this.recordID = recordID;
	}
}
