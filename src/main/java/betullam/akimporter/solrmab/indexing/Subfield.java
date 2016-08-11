package main.java.betullam.akimporter.solrmab.indexing;

public class Subfield {
	
	private String code;
	private String content;
	
	public Subfield();
	
	public Subfield(String code, String content) {
		this.code = code;
		this.content = content;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "Subfield [code=" + code + ", content=" + content + "]";
	}

}