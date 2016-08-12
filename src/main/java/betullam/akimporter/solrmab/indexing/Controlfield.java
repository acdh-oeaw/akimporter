package main.java.betullam.akimporter.solrmab.indexing;

public class Controlfield {
	protected String tag;
	private String content;
	
	public Controlfield() {};
	
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Controlfield)) {
			return false;
		}
		Controlfield other = (Controlfield) obj;
		if (tag == null) {
			if (other.tag != null) {
				return false;
			}
		} else if (!tag.equals(other.tag)) {
			return false;
		}
		return true;
	}
	
	
	
}