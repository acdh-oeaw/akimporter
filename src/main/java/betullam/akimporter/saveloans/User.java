package main.java.betullam.akimporter.saveloans;

public class User {

	private int id;
	private String catUsername;
	private String catId;
	
	
	public User(int id, String catUsername, String catId) {
		this.id = id;
		this.catUsername = catUsername;
		this.catId = catId;
	}

	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getCatUsername() {
		return catUsername;
	}

	public void setCatUsername(String catUsername) {
		this.catUsername = catUsername;
	}

	public String getCatId() {
		return catId;
	}

	public void setCatId(String catId) {
		this.catId = catId;
	}


	@Override
	public String toString() {
		return "User [id=" + id + ", catUsername=" + catUsername + ", catId=" + catId + "]";
	}
	
}
