package main.java.betullam.akimporter.solrmab;

public class PostProcess {

	private int ppId = 0;
	private String ppAction = null;
	private String ppQuery = null;
	private String ppField = null;
	private String ppValue = null;
	
	public PostProcess(int ppId, String ppAction, String ppQuery, String ppField, String ppValue) {
		this.ppId = ppId;
		this.ppAction = ppAction;
		this.ppQuery = ppQuery;
		this.ppField = ppField;
		this.ppValue = ppValue;
	}

	public int getPpId() {
		return ppId;
	}

	public void setPpId(int ppId) {
		this.ppId = ppId;
	}

	public String getPpAction() {
		return ppAction;
	}

	public void setPpAction(String ppAction) {
		this.ppAction = ppAction;
	}

	public String getPpQuery() {
		return ppQuery;
	}

	public void setPpQuery(String ppQuery) {
		this.ppQuery = ppQuery;
	}
	
	public String getPpField() {
		return ppField;
	}

	public void setPpField(String ppField) {
		this.ppField = ppField;
	}

	public String getPpValue() {
		return ppValue;
	}

	public void setPpValue(String ppValue) {
		this.ppValue = ppValue;
	}

	@Override
	public String toString() {
		return "PostProcess [ppId=" + ppId + ", ppAction=" + ppAction + ", ppQuery=" + ppQuery + ", ppField=" + ppField
				+ ", ppValue=" + ppValue + "]";
	}
}