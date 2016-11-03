/**
 * MetsRawRecord class which describes a Mets/Mods record
 * 
 * Copyright (C) AK Bibliothek Wien 2016, Michael Birkner
 * 
 * This file is part of AkImporter.
 * 
 * AkImporter is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AkImporter is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AkImporter.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Michael Birkner <michael.birkner@akwien.at>
 * @license  http://www.gnu.org/licenses/gpl-3.0.html
 * @link     http://wien.arbeiterkammer.at/service/bibliothek/
 */
package main.java.betullam.akimporter.solrmab.indexing;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MetsRawRecord {

	private LinkedHashMap<String,DmdSec> dmdSecs = null;
	private LinkedHashMap<String,StructMapLogical> structMapsLogical = null;
	private LinkedHashMap<String,StructMapPhysical> structMapsPhysical = null;
	private LinkedHashMap<String,StructLink> structLinks = null;

	public LinkedHashMap<String, DmdSec> getDmdSecs() {
		return dmdSecs;
	}

	public void setDmdSecs(LinkedHashMap<String, DmdSec> dmdSecs) {
		this.dmdSecs = dmdSecs;
	}

	public LinkedHashMap<String, StructMapLogical> getStructMapsLogical() {
		return structMapsLogical;
	}

	public void setStructMapsLogical(LinkedHashMap<String, StructMapLogical> structMapsLogical) {
		this.structMapsLogical = structMapsLogical;
	}

	public LinkedHashMap<String, StructMapPhysical> getStructMapsPhysical() {
		return structMapsPhysical;
	}

	public void setStructMapsPhysical(LinkedHashMap<String, StructMapPhysical> structMapsPhysical) {
		this.structMapsPhysical = structMapsPhysical;
	}

	public LinkedHashMap<String, StructLink> getStructLinks() {
		return structLinks;
	}

	public void setStructLinks(LinkedHashMap<String, StructLink> structLinks) {
		this.structLinks = structLinks;
	}
	
	@Override
	public String toString() {
		return "MetsRawRecord [dmdSecs=\n\t" + dmdSecs + "\nstructMapsLogical=\n\t" + structMapsLogical
				+ "\nstructMapsPhysical=\n\t" + structMapsPhysical + "\nstructLinks=\n\t" + structLinks + "]";
	}



	public class DmdSec {
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

		@Override
		public String toString() {
			return "DmdSec [classifications=" + classifications + "publisher=" + publisher + ", place=" + place
					+ ", year=" + year + ", volume=" + volume + ", issueNo=" + issueNo + ", sortNo=" + sortNo
					+ ", title=" + title + ", subTitle=" + subTitle + ", acNo=" + acNo + ", akIdentifier="
					+ akIdentifier + ", languageTerm=" + languageTerm + ", abstractTexts=" + abstractTexts
					+ ", participants=" + participants + "]\n\t";
		}

	}

	public class StructMapLogical {
		String logId = null;
		String type = null;
		int level = 0;
		public String getLogId() {
			return logId;
		}
		public void setLogId(String logId) {
			this.logId = logId;
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
		
		@Override
		public String toString() {
			return "StructMapLogical [logId=" + logId + ", type=" + type + ", level=" + level + "]\n\t";
		}
	}

	public class StructMapPhysical {
		String dmdPhysId = null;
		String physId = null;
		String contentId = null;
		int order = 0;
		String orderLabel = null;
		String type = null;
		public String getDmdPhysId() {
			return dmdPhysId;
		}
		public void setDmdPhysId(String dmdPhysId) {
			this.dmdPhysId = dmdPhysId;
		}
		public String getPhysId() {
			return physId;
		}
		public void setPhysId(String physId) {
			this.physId = physId;
		}
		public String getContentId() {
			return contentId;
		}
		public void setContentId(String contentId) {
			this.contentId = contentId;
		}
		public int getOrder() {
			return order;
		}
		public void setOrder(int order) {
			this.order = order;
		}
		public String getOrderLabel() {
			return orderLabel;
		}
		public void setOrderLabel(String orderLabel) {
			this.orderLabel = orderLabel;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		
		@Override
		public String toString() {
			return "StructMapPhysical [dmdPhysId=" + dmdPhysId + ", physId=" + physId + ", contentId=" + contentId
					+ ", order=" + order + ", orderLabel=" + orderLabel + ", type=" + type + "]\n\t";
		}
	}
	
	public class StructLink {
		String smLinkTo = null;
		String smLinkFrom = null;
		
		public String getSmLinkTo() {
			return smLinkTo;
		}
		public void setSmLinkTo(String smLinkTo) {
			this.smLinkTo = smLinkTo;
		}
		public String getSmLinkFrom() {
			return smLinkFrom;
		}
		public void setSmLinkFrom(String smLinkFrom) {
			this.smLinkFrom = smLinkFrom;
		}
		
		@Override
		public String toString() {
			return "StructLink [smLinkTo=" + smLinkTo + ", smLinkFrom=" + smLinkFrom + "]\n\t";
		}
	}

	public class Participant {
		private String familyName;
		private String givenName;
		private String role;
		private String authorityId;

		public String getFamilyName() {
			return familyName;
		}
		public void setFamilyName(String familyName) {
			this.familyName = familyName;
		}
		public String getGivenName() {
			return givenName;
		}
		public void setGivenName(String givenName) {
			this.givenName = givenName;
		}
		public String getRole() {
			return role;
		}
		public void setRole(String role) {
			this.role = role;
		}
		public String getAuthorityId() {
			return authorityId;
		}
		public void setAuthorityId(String authorityId) {
			this.authorityId = authorityId;
		}

		@Override
		public String toString() {
			return "Participant [familyName=" + familyName + ", givenName=" + givenName + ", role=" + role
					+ ", authorityId=" + authorityId + "]";
		}		

	}


	/*
	String topParentTitle;
	String topParentType;
	String topParentContentId;
	String topParentLogId;
	// String topTitle;
	// String topSubtitle;
	// With Rule from Marc? String topTitleSort;
	// List<Person> topPersons;
	// String topYear;
	// String topVolume;
	// String topIssueNo;
	// String topSortNo;
	// String topPlace;
	// String topPublisher;
	// String topLanguage;
	// List<String> topClassification;
	// String topType;
	// String topAkIdentifier;
	// String topAcNo;
	String topGoobiId;
	//String topContentId;
	// String topDmdLogId;
	// String topLogId;
	//String topPhysId;
	List<Child> childs;

	private class Child {
		// String childTitle;
		// String childSubtitle;
		// With Rule from Marc? String childTitleSort;
		// List<Person> childPersons;
		// List<String> childAbstracts;
		// String childLanguage;
		// List<String> childClassification;
		String childFromPage;
		String childToPage;
		// String childAkIdentifier;
		// String childType;
		//String childContentId;
		// String childDmdLogId;
		// String childLogId;
		//String childPhysId;
	}

	private class Person {
		// String personFirstName;
		// String personLastName;
		// String personRole;
		String personAuthorityId;
	}
	 */

}
