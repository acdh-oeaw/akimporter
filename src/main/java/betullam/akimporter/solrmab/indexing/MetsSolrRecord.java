package main.java.betullam.akimporter.solrmab.indexing;

import java.util.ArrayList;
import java.util.List;

import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.Participant;

public class MetsSolrRecord {
	
	private boolean isParentOfParent = false;
	private String parentOfParentUrn = null;
	private String parentOfParentTitle = null;
	private String parentOfParentType = null;
	private int parentOfParentLevel = 0;
	
	private boolean isParent = false;
	private String parentId = null;
	private String parentUrn = null;
	private String parentTitle = null;
	private String parentSubtitle = null;
	private String parentType = null;
	private int parentLevel = 0;
	
	private boolean isChild = false;
	private List<String> classifications = new ArrayList<String>();
	private String publisher = null;
	private String place = null;
	private String year = null;
	private String volume = null;
	private String issueNo = null;
	private String sortNo = null;
	private String title = null;
	private String subTitle = null;
	private String acNo = null;
	private String akIdentifier = null;
	//private String reviewedBookTitle = null;
	private String languageTerm = null;
	private List<String> abstractTexts = new ArrayList<String>();
	private List<Participant> participants = new ArrayList<Participant>();
	private String type = null;
	int level = 0;
	private String urn = null;
	private int order = 0;
	private String orderLabelFrom = null;
	private String orderLabelTo = null;
	private String logId = null;
	private int orderLogId = 0;
	

	public boolean isParentOfParent() {
		return isParentOfParent;
	}
	public void setIsParentOfParent(boolean isParentOfParent) {
		this.isParentOfParent = isParentOfParent;
	}
	public String getParentOfParentUrn() {
		return parentOfParentUrn;
	}
	public void setParentOfParentUrn(String parentOfParentUrn) {
		this.parentOfParentUrn = parentOfParentUrn;
	}
	public String getParentOfParentTitle() {
		return parentOfParentTitle;
	}
	public void setParentOfParentTitle(String parentOfParentTitle) {
		this.parentOfParentTitle = parentOfParentTitle;
	}
	public String getParentOfParentType() {
		return parentOfParentType;
	}
	public void setParentOfParentType(String parentOfParentType) {
		this.parentOfParentType = parentOfParentType;
	}
	public int getParentOfParentLevel() {
		return parentOfParentLevel;
	}
	public void setParentOfParentLevel(int parentOfParentLevel) {
		this.parentOfParentLevel = parentOfParentLevel;
	}
	
	
	public boolean isParent() {
		return isParent;
	}
	public void setIsParent(boolean isParent) {
		this.isParent = isParent;
	}
	public String getParentId() {
		return parentId;
	}
	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
	public String getParentUrn() {
		return parentUrn;
	}
	public void setParentUrn(String parentUrn) {
		this.parentUrn = parentUrn;
	}
	public String getParentTitle() {
		return parentTitle;
	}
	public void setParentTitle(String parentTitle) {
		this.parentTitle = parentTitle;
	}
	public String getParentSubtitle() {
		return parentSubtitle;
	}
	public void setParentSubtitle(String parentSubtitle) {
		this.parentSubtitle = parentSubtitle;
	}
	public String getParentType() {
		return parentType;
	}
	public void setParentType(String parentType) {
		this.parentType = parentType;
	}
	public int getParentLevel() {
		return parentLevel;
	}
	public void setParentLevel(int parentLevel) {
		this.parentLevel = parentLevel;
	}
	
	
	public boolean isChild() {
		return isChild;
	}
	public void setIsChild(boolean isChild) {
		this.isChild = isChild;
	}
	protected List<String> getClassifications() {
		return classifications;
	}
	protected void setClassifications(List<String> classifications) {
		this.classifications = classifications;
	}
	public String getPublisher() {
		return publisher;
	}
	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}
	public String getPlace() {
		return place;
	}
	public void setPlace(String place) {
		this.place = place;
	}
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public String getVolume() {
		return volume;
	}
	public void setVolume(String volume) {
		this.volume = volume;
	}
	public String getIssueNo() {
		return issueNo;
	}
	public void setIssueNo(String issueNo) {
		this.issueNo = issueNo;
	}
	public String getSortNo() {
		return sortNo;
	}
	public void setSortNo(String sortNo) {
		this.sortNo = sortNo;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getSubTitle() {
		return subTitle;
	}
	public void setSubTitle(String subTitle) {
		this.subTitle = subTitle;
	}
	public String getAcNo() {
		return acNo;
	}
	public void setAcNo(String acNo) {
		this.acNo = acNo;
	}
	public String getAkIdentifier() {
		return akIdentifier;
	}
	public void setAkIdentifier(String akIdentifier) {
		this.akIdentifier = akIdentifier;
	}
	public String getLanguageTerm() {
		return languageTerm;
	}
	public void setLanguageTerm(String languageTerm) {
		this.languageTerm = languageTerm;
	}
	public List<String> getAbstractTexts() {
		return abstractTexts;
	}
	public void setAbstractTexts(List<String> abstractTexts) {
		this.abstractTexts = abstractTexts;
	}
	public List<Participant> getParticipants() {
		return participants;
	}
	public void setParticipants(List<Participant> participants) {
		this.participants = participants;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public int getLevel() {
		return level;
	}
	public void setLevel(int level) {
		this.level = level;
	}
	public String getUrn() {
		return urn;
	}
	public void setUrn(String urn) {
		this.urn = urn;
	}
	public int getOrder() {
		return order;
	}
	public void setOrder(int order) {
		this.order = order;
	}
	public String getOrderLabelFrom() {
		return orderLabelFrom;
	}
	public void setOrderLabelFrom(String orderLabelFrom) {
		this.orderLabelFrom = orderLabelFrom;
	}
	public String getOrderLabelTo() {
		return orderLabelTo;
	}
	public void setOrderLabelTo(String orderLabelTo) {
		this.orderLabelTo = orderLabelTo;
	}
	public String getLogId() {
		return logId;
	}
	public void setLogId(String logId) {
		this.logId = logId;
	}
	public int getOrderLogId() {
		return orderLogId;
	}
	public void setOrderLogId(int orderLogId) {
		this.orderLogId = orderLogId;
	}
	
	
	@Override
	public String toString() {
		return "MetsSolrRecord [isParentOfParent=" + isParentOfParent + ", parentOfParentUrn=" + parentOfParentUrn
				+ ", parentOfParentTitle=" + parentOfParentTitle + ", parentOfParentType=" + parentOfParentType
				+ ", parentOfParentLevel=" + parentOfParentLevel + ", isParent=" + isParent + ", parentId=" + parentId
				+ ", parentUrn=" + parentUrn + ", parentTitle=" + parentTitle + ", parentSubtitle=" + parentSubtitle
				+ ", parentType=" + parentType + ", parentLevel=" + parentLevel + ", isChild=" + isChild
				+ ", classifications=" + classifications + ", publisher=" + publisher + ", place=" + place + ", year="
				+ year + ", volume=" + volume + ", issueNo=" + issueNo + ", sortNo=" + sortNo + ", title=" + title
				+ ", subTitle=" + subTitle + ", acNo=" + acNo + ", akIdentifier=" + akIdentifier + ", languageTerm="
				+ languageTerm + ", abstractTexts=" + abstractTexts + ", participants=" + participants + ", type="
				+ type + ", level=" + level + ", urn=" + urn + ", order=" + order + ", orderLabelFrom=" + orderLabelFrom
				+ ", orderLabelTo=" + orderLabelTo + ", logId=" + logId + ", orderLogId=" + orderLogId + "]";
	}
}