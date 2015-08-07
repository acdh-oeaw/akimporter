package betullam.akimporter.solrmab2;

import java.util.List;

public class Record {

	private List<Mabfield> mabfields;
	private String recordID;
	private String indexTimestamp;
	

	public Record() {}
	

	public List<Mabfield> getMabfields() {
		return mabfields;
	}

	public void setMabfields(List<Mabfield> mabfields) {
		this.mabfields = mabfields;
	}

	public String getRecordID() {
		return recordID;
	}

	public void setRecordID(String recordID) {
		this.recordID = recordID;
	}

	public String getIndexTimestamp() {
		return indexTimestamp;
	}

	public void setIndexTimestamp(String indexTimestamp) {
		this.indexTimestamp = indexTimestamp;
	}

	@Override
	public String toString() {
		return "Record [mabfields=" + mabfields + ", recordID=" + recordID + ", indexTimestamp=" + indexTimestamp + "]";
	}


	
	
	
}
