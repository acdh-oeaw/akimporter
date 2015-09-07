package betullam.akimporter.solrmab.indexing;


public class Mabfield {
	
	private String fieldname; // trim(datafield-tag + datafield-ind1 + datafield-ind2) + trim(subfield-code)
	private String fieldvalue; // trim(subfield-value)
	
	public Mabfield() {}

	public Mabfield(String fieldname, String fieldvalue) {
		this.setFieldname(fieldname);
		this.setFieldvalue(fieldvalue);
	}
	

	public String getFieldname() {
		return this.fieldname;
	}

	public void setFieldname(String fieldname) {
		this.fieldname = fieldname;
	}

	public String getFieldvalue() {
		return this.fieldvalue;
	}

	public void setFieldvalue(String fieldvalue) {
		this.fieldvalue = fieldvalue;
	}

	@Override
	public String toString() {
		return "Mabfield [fieldname=" + fieldname + ", fieldvalue="
				+ fieldvalue + "]";
	}
	
	

}
