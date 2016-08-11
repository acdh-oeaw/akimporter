package main.java.betullam.akimporter.solrmab.indexing;

public class Controlfield {
	private String tag;
	private String content;
	
	public Controlfield();
	
	public Controlfield(String tag, String content) {
		this.tag = tag;
		this.content = content;
	}
	
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}

	@Override
	public String toString() {
		return "Controlfield [tag=" + tag + ", content=" + content + "]";
	}
}