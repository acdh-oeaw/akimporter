package main.java.betullam.akimporter.solrmab.indexing;

public class Leader {

	private String tag;
	private String content;
	
	public Leader() {};
	public Leader(String tag, String content) {
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
	
	
	/**
	 * Check if a raw leader is contained in a properties object (represents a line in mab.properties)
	 * @param 	propertiesObject	PropertiesObject: A properties object representing a line in mab.properties
	 * @return						boolean: true if the properties object contains the leader this method is applied on, false otherwise
	 */
	public boolean isContainedInPropertiesObject(PropertiesObject propertiesObject) {

		// Return false if propertiesObject is null
		if (propertiesObject == null) {
			return false;
		}
		
		// Returns false if propertiesObject is no instance of PropertiesObject
		if (!(propertiesObject instanceof PropertiesObject)) {
			return false;
		}

		boolean returnValue = false;

		if (this.equals(propertiesObject.getLeader())) {
			returnValue = true;
		}

		return returnValue;
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
		if (!(obj instanceof Leader)) {
			return false;
		}
		Leader other = (Leader) obj;
		if (tag == null) {
			if (other.tag != null) {
				return false;
			}
		} else if (!tag.equals(other.tag)) {
			return false;
		}
		return true;
	}
	
	
	@Override
	public String toString() {
		return "Leader [tag=" + tag + ", content=" + content + "]";
	}
	
}