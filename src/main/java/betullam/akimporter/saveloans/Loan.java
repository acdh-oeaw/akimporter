package main.java.betullam.akimporter.saveloans;

public class Loan {
	
	private String ilsLoanId;
	private int userId;
	private String ilsUserId;
	private String itemId;
	private String title;
	private String author;
	private String publicationYear;
	private String loanDate;
	private String dueDate;
	private String library;
	private String locationCode;
	private String callNo;
	private String barcode;
	
	
	public Loan() {};
	
	public Loan(String ilsLoanId, int userId, String ilsUserId, String itemId, String title, String author,
			String publicationYear, String loanDate, String dueDate, String library, String locationCode, String callNo,
			String barcode) {
		this.ilsLoanId = ilsLoanId;
		this.userId = userId;
		this.ilsUserId = ilsUserId;
		this.itemId = itemId;
		this.title = title;
		this.author = author;
		this.publicationYear = publicationYear;
		this.loanDate = loanDate;
		this.dueDate = dueDate;
		this.library = library;
		this.locationCode = locationCode;
		this.callNo = callNo;
		this.barcode = barcode;
	}

	
	public String getIlsLoanId() {
		return ilsLoanId;
	}

	public void setIlsLoanId(String ilsLoanId) {
		this.ilsLoanId = ilsLoanId;
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	public String getIlsUserId() {
		return ilsUserId;
	}

	public void setIlsUserId(String ilsUserId) {
		this.ilsUserId = ilsUserId;
	}

	public String getItemId() {
		return itemId;
	}

	public void setItemId(String itemId) {
		this.itemId = itemId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getPublicationYear() {
		return publicationYear;
	}

	public void setPublicationYear(String publicationYear) {
		this.publicationYear = publicationYear;
	}

	public String getLoanDate() {
		return loanDate;
	}

	public void setLoanDate(String loanDate) {
		this.loanDate = loanDate;
	}

	public String getDueDate() {
		return dueDate;
	}

	public void setDueDate(String dueDate) {
		this.dueDate = dueDate;
	}

	public String getLibrary() {
		return library;
	}

	public void setLibrary(String library) {
		this.library = library;
	}

	public String getLocationCode() {
		return locationCode;
	}

	public void setLocationCode(String locationCode) {
		this.locationCode = locationCode;
	}

	public String getCallNo() {
		return callNo;
	}

	public void setCallNo(String callNo) {
		this.callNo = callNo;
	}

	public String getBarcode() {
		return barcode;
	}

	public void setBarcode(String barcode) {
		this.barcode = barcode;
	}

	
	@Override
	public String toString() {
		return "Loan [ilsLoanId=" + ilsLoanId + ", userId=" + userId + ", ilsUserId=" + ilsUserId + ", itemId=" + itemId
				+ ", title=" + title + ", author=" + author + ", publicationYear=" + publicationYear + ", loanDate="
				+ loanDate + ", dueDate=" + dueDate + ", library=" + library + ", locationCode=" + locationCode
				+ ", callNo=" + callNo + ", barcode=" + barcode + "]";
	}
	
}
