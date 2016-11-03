package main.java.betullam.akimporter.solrmab.indexing;

import java.util.ArrayList;
import java.util.List;

import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.Participant;

public class MetsSolrRecord {
	
	// DmdSec
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
	//private String dmdLogId = null;
	
	// Logical StructMap
	//private String logId = null;
	private String type = null;
	int level = 0;
	
	// Physical StructMap
	private String urn = null;
	private int order = 0;
	private String orderLabelFrom = null;
	private String orderLabelTo = null;
	
	
	public List<String> getClassifications() {
		return classifications;
	}
	public void setClassifications(List<String> classifications) {
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
	/*
	public String getDmdLogId() {
		return dmdLogId;
	}
	public void setDmdLogId(String dmdLogId) {
		this.dmdLogId = dmdLogId;
	}
	public String getLogId() {
		return logId;
	}
	public void setLogId(String logId) {
		this.logId = logId;
	}
	*/
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
	@Override
	public String toString() {
		return "MetsSolrRecord [\n\tclassifications=" + classifications + "\n\tpublisher=" + publisher + "\n\tplace=" + place
				+ "\n\tyear=" + year + "\n\tvolume=" + volume + "\n\tissueNo=" + issueNo + "\n\tsortNo=" + sortNo + "\n\ttitle="
				+ title + "\n\tsubTitle=" + subTitle + "\n\tacNo=" + acNo + "\n\takIdentifier=" + akIdentifier
				+ "\n\tlanguageTerm=" + languageTerm + "\n\tabstractTexts=" + abstractTexts + "\n\tparticipants="
				+ participants + "\n\ttype=" + type + "\n\tlevel=" + level + "\n\turn=" + urn + "\n\torder=" + order
				+ "\n\torderLabelFrom=" + orderLabelFrom + "\n\torderLabelTo=" + orderLabelTo + "\n]";
	}	
	
	
}
