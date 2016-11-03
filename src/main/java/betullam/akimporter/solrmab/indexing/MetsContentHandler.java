/**
 * Parses the contents of Mets/Mods files.
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
import java.util.Map.Entry;

import org.apache.solr.client.solrj.SolrServer;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.DmdSec;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.Participant;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.StructLink;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.StructMapLogical;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.StructMapPhysical;

public class MetsContentHandler implements ContentHandler {

	private SolrServer solrServer;
	private String elementContent;
	private String timeStamp;
	private boolean print = true;

	// XML sections we need to process
	private boolean isRecord = false;
	private boolean isDmdSec = false;
	private boolean isLogicalStructMap = false;
	private boolean isPhysicalStructMap = false;
	private boolean isStructLink = false;
	private boolean isClassification = false;
	private boolean isOriginInfo = false;
	private boolean isPublisher = false;
	private boolean isPlace = false;
	private boolean isYear = false;
	private boolean isOriginInfoPublication = false;
	private boolean isPublisherPublication = false;
	private boolean isPlacePublication = false;
	private boolean isYearPublication = false;
	private boolean isTitleInfo = false;
	private boolean isSubtitle = false;
	private boolean isTitle = false;
	private boolean isRelatedItem = false;
	private boolean isReview = false;
	private boolean isReviewedBookTitle = false;
	private boolean isRecordInfo = false;
	private boolean isRecordIdentifier = false;
	private boolean isAcNo = false;
	private boolean isAkIdentifier = false;
	private boolean isPart = false;
	private boolean isIssue = false;
	private boolean isVolume = false;
	private boolean isNumber = false;
	private boolean isLanguageTerm = false;
	private boolean isAbstract = false;
	private boolean isName = false;
	private boolean isPersonalName = false;
	private boolean isCorporateName = false;
	private boolean isRoleTerm = false;
	private boolean isFamilyNamePart = false;
	private boolean isGivenNamePart = false;


	// Attribute values that we get in startElement-Method
	private String dmdLogIdDmdSec = null;
	private String dmdLogId_logicalStructMap = null;
	private String logId_logicalStructMap = null;
	private int level = 0;
	private String physId_physicalStructMap = null;
	private String smLinkTo = null;


	private MetsRawRecord metsRawRecord = null;
	private LinkedHashMap<String, DmdSec> dmdSecs = null;
	private DmdSec dmdSec = null;
	private LinkedHashMap<String, StructMapLogical> structMapsLogical = null;
	private StructMapLogical structMapLogical = null;
	private LinkedHashMap<String, StructMapPhysical> structMapsPhysical = null;
	private StructMapPhysical structMapPhysical = null;
	private LinkedHashMap<String, StructLink> structLinks = null;
	private StructLink structLink = null;
	private List<String> classifications = null;
	private Participant participant = null;
	private List<Participant> participants = null;


	public MetsContentHandler(SolrServer solrServer, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;
	}

	@Override
	public void startDocument() throws SAXException {

	}


	@Override
	public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {

		// Clear element content for fresh start
		elementContent = "";

		if (qName.equals("record")) {
			isRecord = true;
			metsRawRecord = new MetsRawRecord();
			dmdSecs = new LinkedHashMap<String, DmdSec>();
			structMapsLogical = new LinkedHashMap<String, StructMapLogical>();
			structMapsPhysical = new LinkedHashMap<String, StructMapPhysical>();
			structLinks = new LinkedHashMap<String, StructLink>();
		}


		if (isRecord) {

			if (qName.equals("mets:dmdSec")) {
				isDmdSec = true;
				dmdSec = metsRawRecord.new DmdSec();
				classifications = new ArrayList<String>();
				participants = new ArrayList<Participant>();
			}

			if (qName.equals("mets:structMap") && atts.getValue("TYPE") != null && atts.getValue("TYPE").equals("LOGICAL")) {
				isLogicalStructMap = true;
				level = 0;
			}

			if (qName.equals("mets:structMap") && atts.getValue("TYPE") != null && atts.getValue("TYPE").equals("PHYSICAL")) {
				isPhysicalStructMap = true;
			}

			if (qName.equals("mets:structLink")) {
				isStructLink = true;
			}
		}


		if (isDmdSec) {

			if (atts.getValue("ID") != null) {
				dmdLogIdDmdSec = atts.getValue("ID");
			}

			if (qName.equals("mods:classification") && atts.getValue("authority") != null && atts.getValue("authority").equals("sswd")) {
				isClassification = true;
			}

			if (qName.equals("mods:originInfo")) {
				isOriginInfo = true;
				if (atts.getValue("eventType") != null && atts.getValue("eventType").equals("publication")) {
					isOriginInfoPublication = true;
				}
				
			}

			if (isOriginInfo && !isOriginInfoPublication) {
				if (qName.equals("mods:publisher")) {
					isPublisher = true;
				}

				if (qName.equals("mods:placeTerm")) {
					isPlace = true;
				}

				if (qName.equals("mods:dateIssued")) {
					isYear = true;
				}
			}
			
			if (isOriginInfo && isOriginInfoPublication) {
				if (qName.equals("mods:publisher")) {
					isPublisherPublication = true;
				}

				if (qName.equals("mods:placeTerm")) {
					isPlacePublication = true;
				}

				if (qName.equals("mods:dateIssued")) {
					isYearPublication = true;
				}
			}

			if (qName.equals("mods:relatedItem")) {
				isRelatedItem = true;

				if (atts.getValue("type") != null && atts.getValue("type").equals("reviewOf")) {
					isReview = true;
				}
			}

			if (isReview) {
				if (qName.equals("mods:title")) {
					isReviewedBookTitle = true;
				}
			}

			if (qName.equals("mods:part")) {
				isPart = true;
			}

			if (isPart) {

				if (atts.getValue("order") != null) {
					//sortNo = atts.getValue("order");
					dmdSec.setSortNo(atts.getValue("order"));
					//System.out.println("sortNo: " + sortNo);
				}

				if (qName.equals("mods:detail") && atts.getValue("type") != null && atts.getValue("type").equals("issue")) {
					isIssue = true;
				}

				if (qName.equals("mods:detail") && atts.getValue("type") != null && atts.getValue("type").equals("volume")) {
					isVolume = true;
				}

				if (qName.equals("mods:number")) {
					isNumber = true;
				}
			}

			if (qName.equals("mods:titleInfo") && atts.getLength() == 0) {
				isTitleInfo = true;
			}

			if (isTitleInfo) {
				if (qName.equals("mods:title") && atts.getLength() == 0) {
					isTitle = true;
				}

				if (qName.equals("mods:subTitle")) {
					isSubtitle = true;
				}
			}

			if (qName.equals("mods:recordInfo")) {
				isRecordInfo = true;
			}

			if (isRecordInfo) {
				if (qName.equals("mods:recordIdentifier")) {
					isRecordIdentifier = true;
					if (atts.getValue("source") != null) {
						if (atts.getValue("source").equals("gbv-ppn")) {
							isAcNo = true;
						}
						if (atts.getValue("source").equals("ak_identifier")) {
							isAkIdentifier = true;
						}
					}
				}
			}

			if (qName.equals("mods:languageTerm")) {
				isLanguageTerm = true;
			}

			if (qName.equals("mods:abstract")) {
				isAbstract = true;
			}

			if (qName.equals("mods:name")) {
				isName = true;

				if (atts.getValue("type") != null && atts.getValue("type").equals("personal")) {
					isPersonalName = true;
					participant = metsRawRecord.new Participant();
				}

				if (atts.getValue("type") != null && atts.getValue("type").equals("corporate")) {
					isCorporateName = true;
					participant = metsRawRecord.new Participant();
				}
			}

			if (isName) {
				if (qName.equals("mods:roleTerm")) {
					isRoleTerm = true;
				}

				if (qName.equals("mods:namePart")) {
					if (atts.getValue("type") != null && atts.getValue("type").equals("family")) {
						isFamilyNamePart = true;
					}

					if (atts.getValue("type") != null && atts.getValue("type").equals("given")) {
						isGivenNamePart = true;
					}
				}
			}
		}


		if (isLogicalStructMap) {

			if (qName.equals("mets:div")) {
				level = level + 1;
				structMapLogical = metsRawRecord.new StructMapLogical();
				structMapLogical.setLevel(level);

				if (atts.getValue("DMDID") != null) {
					dmdLogId_logicalStructMap = atts.getValue("DMDID");
					structMapLogical.setDmdLogId(dmdLogId_logicalStructMap);
				} else {
					dmdLogId_logicalStructMap = null;
				}

				if (atts.getValue("ID") != null) {
					logId_logicalStructMap = atts.getValue("ID");
					structMapLogical.setLogId(logId_logicalStructMap);
				} else {
					logId_logicalStructMap = null;
				}

				if (atts.getValue("TYPE") != null) {
					structMapLogical.setType(atts.getValue("TYPE"));
				}

				if (dmdLogId_logicalStructMap != null) {
					structMapsLogical.put(dmdLogId_logicalStructMap, structMapLogical);
				} else {
					structMapsLogical.put(logId_logicalStructMap, structMapLogical);
				}
				//System.out.println(structMapLogical);
			}
		}


		if (isPhysicalStructMap) {
			if (qName.equals("mets:div")) {
				structMapPhysical = metsRawRecord.new StructMapPhysical();

				if (atts.getValue("ID") != null) {
					physId_physicalStructMap = atts.getValue("ID");
					structMapPhysical.setPhysId(physId_physicalStructMap);
				} else {
					physId_physicalStructMap = null;
				}

				if (atts.getValue("DMDID") != null) {
					structMapPhysical.setDmdPhysId(atts.getValue("DMDID"));
				}

				if (atts.getValue("CONTENTIDS") != null) {
					structMapPhysical.setContentId(atts.getValue("CONTENTIDS"));
				}

				if (atts.getValue("ORDER") != null) {
					structMapPhysical.setOrder(Integer.valueOf(atts.getValue("ORDER")));
				}

				if (atts.getValue("ORDERLABEL") != null) {
					structMapPhysical.setOrderLabel(atts.getValue("ORDERLABEL"));
				}

				if (atts.getValue("TYPE") != null) {
					structMapPhysical.setType(atts.getValue("TYPE"));
				}

				structMapsPhysical.put(physId_physicalStructMap, structMapPhysical);
				//System.out.println(structMapPhysical);
			}
		}


		if (isStructLink) {
			if (qName.equals("mets:smLink")) {
				structLink = metsRawRecord.new StructLink();

				if (atts.getValue("xlink:to") != null) {
					smLinkTo = atts.getValue("xlink:to");
					structLink.setSmLinkTo(smLinkTo);

				} else {

					smLinkTo = null;
				}

				if (atts.getValue("xlink:from") != null) {
					structLink.setSmLinkFrom(atts.getValue("xlink:from"));
				}

				structLinks.put(smLinkTo, structLink);
				//System.out.println(structLink);
			}
		}



	}


	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {


		if (isClassification) {
			classifications.add(elementContent);
		}

		if (isPublisherPublication) {
			dmdSec.setPublisherPublication(elementContent);
		}

		if (isPlacePublication) {
			dmdSec.setPlacePublication(elementContent);
		}

		if (isYearPublication) {
			dmdSec.setYearPublication(elementContent);
		}
		
		if (isPublisher) {
			dmdSec.setPublisher(elementContent);
		}

		if (isPlace) {
			dmdSec.setPlace(elementContent);
		}

		if (isYear) {
			dmdSec.setYear(elementContent);
		}
		
		if (isVolume && isNumber) {
			dmdSec.setVolume(elementContent);
		}

		if (isIssue && isNumber) {
			dmdSec.setIssueNo(elementContent);
		}

		if (isTitle && !isReviewedBookTitle) {
			dmdSec.setTitle(elementContent);
		}

		/*
		if (isReviewedBookTitle) {
			reviewedBookTitle = elementContent;
			System.out.println("reviewedBookTitle: " + reviewedBookTitle);
		}
		 */

		if (isSubtitle) {
			dmdSec.setSubTitle(elementContent);
		}

		if (isPersonalName && isGivenNamePart) {
			participant.setGivenName(elementContent);

		}

		if ((isPersonalName || isCorporateName) && isFamilyNamePart) {
			participant.setFamilyName(elementContent);
		}

		if (isRoleTerm) {
			participant.setRole(elementContent);
		}

		if (isRecordIdentifier && !isRelatedItem) {
			if (isAcNo) {
				dmdSec.setAcNo(elementContent);
			}

			if (isAkIdentifier) {
				dmdSec.setAkIdentifier(elementContent);
			}
		}

		if (isLanguageTerm) {
			dmdSec.setLanguageTerm(elementContent);
		}

		if (isAbstract) {
			dmdSec.getAbstractTexts().add(elementContent);
		}





		if (qName.equals("record")) {
			metsRawRecord.setDmdSecs(dmdSecs);
			metsRawRecord.setStructMapsLogical(structMapsLogical);
			metsRawRecord.setStructMapsPhysical(structMapsPhysical);
			metsRawRecord.setStructLinks(structLinks);
			//System.out.println(metsRawRecord);

			isRecord = false;
			System.out.println("\n------------------------------------------------------\n");

			// TODO: Change raw record to a record that can be indexed to Solr.
			List<MetsSolrRecord> metsSolrRecords = getMetsSolrRecord(metsRawRecord);
			System.out.println(metsSolrRecords);
			//System.out.println(metsRawRecord);
		}

		if (qName.equals("mets:dmdSec")) {
			dmdSec.setClassifications(classifications);
			dmdSec.setParticipants(participants);
			dmdSecs.put(dmdLogIdDmdSec, dmdSec);
			//System.out.println(dmdSecs);
			isDmdSec = false;
		}

		if (qName.equals("mets:structMap")) {
			isLogicalStructMap = false;
			isPhysicalStructMap = false;
		}

		if (qName.equals("mets:div")) {
			if (isLogicalStructMap) {
				level = level - 1;
			}
		}

		if (qName.equals("mets:structLink")) {
			isStructLink = false;
		}

		if (qName.equals("mods:originInfo")) {
			isOriginInfo = false;
			isOriginInfoPublication = false;
		}

		if (qName.equals("mods:classification")) {
			isClassification = false;
		}

		if (qName.equals("mods:relatedItem")) {
			isReview = false;
			isRelatedItem = false;
		}

		if (qName.equals("mods:titleInfo")) {
			isTitleInfo = false;
		}

		if (qName.equals("mods:title")) {
			isTitle = false;
			isReviewedBookTitle = false;
		}

		if (qName.equals("mods:subTitle")) {
			isSubtitle = false;
		}

		if (qName.equals("mods:publisher")) {
			isPublisher = false;
			isPublisherPublication = false;
		}

		if (qName.equals("mods:placeTerm")) {
			isPlace = false;
			isPlacePublication = false;
		}

		if (qName.equals("mods:dateIssued")) {
			isYear = false;
			isYearPublication = false;
		}

		if (qName.equals("mods:recordInfo")) {
			isRecordInfo = false;
		}

		if (qName.equals("mods:recordIdentifier")) {
			isRecordIdentifier = false;
			isAcNo = false;
			isAkIdentifier = false;
		}

		if (qName.equals("mods:part")) {
			isPart = false;
		}

		if (qName.equals("mods:detail")) {
			isIssue = false;
			isVolume = false;
		}

		if (qName.equals("mods:number")) {
			isNumber = false;
		}

		if (qName.equals("mods:languageTerm")) {
			isLanguageTerm = false;
		}

		if (qName.equals("mods:abstract")) {
			isAbstract = false;
		}

		if (qName.equals("mods:name")) {
			participants.add(participant);
			isName = false;
			isPersonalName = false;
			isCorporateName = false;
		}

		if (qName.equals("mods:roleTerm")) {
			isRoleTerm = false;
		}

		if (qName.equals("mods:namePart")) {
			isFamilyNamePart = false;
			isGivenNamePart = false;
		}



	}


	@Override
	public void endDocument() throws SAXException {	
	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {	
		elementContent += new String(ch, start, length).replaceAll("\\s+", " ");
	}


	/**
	 * Prints the specified text to the console if "print" is true.
	 * 
	 * @param print		boolean: true if the text should be print, false otherwise
	 * @param text		String: a text message to print.
	 */
	/*
	private void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}
	 */

	private List<MetsSolrRecord> getMetsSolrRecord(MetsRawRecord metsRawRecord) {
		// TODO: Change raw record to a record that can be indexed to Solr.

		List<MetsSolrRecord> metsSolrRecords = new ArrayList<MetsSolrRecord>();

		LinkedHashMap<String,DmdSec> dmdSecs = metsRawRecord.getDmdSecs();
		LinkedHashMap<String,StructMapLogical> structMapsLogical = metsRawRecord.getStructMapsLogical();
		LinkedHashMap<String,StructMapPhysical> structMapsPhysical = metsRawRecord.getStructMapsPhysical();
		LinkedHashMap<String,StructLink> structLinks = metsRawRecord.getStructLinks();

		//System.out.println(structMapsLogical);

		// Get the StructMapLogical that has a DMD-ID and also has the lowest level. This is the topmost element that contains data
		// like publication year and place, volume no., issue no., etc. that are relevant for all other records like articles or chapters.
		//int lowestLevel = 1;
		String lowestLevelDmdLogId = null;
		if (structMapsLogical != null) {
			// Get first level of a StructMapLogical that also has a DMDLOG-ID. That is our lowest level we have:
			for (Entry<String,StructMapLogical> structMapLogicalMap : structMapsLogical.entrySet()) {
				StructMapLogical structMapLogical = structMapLogicalMap.getValue();
				if (structMapLogical.getDmdLogId() != null) {
					//lowestLevel = structMapLogical.getLevel();
					lowestLevelDmdLogId = structMapLogical.getDmdLogId();
					break;
				}
			}
		}
		//System.out.println(lowestLevelDmdLogId);
		
		DmdSec topDmdSec = dmdSecs.get(lowestLevelDmdLogId);
		//System.out.println(topDmdSec);
		String topAcNo = topDmdSec.getAcNo();
		String topAkIdentifier = topDmdSec.getAkIdentifier();
		List<String> topClassifications = topDmdSec.getClassifications();
		String topLanguageTerm = topDmdSec.getLanguageTerm();
		String topPublisher = (topDmdSec.getPublisherPublication() != null) ? topDmdSec.getPublisherPublication() : topDmdSec.getPublisher();
		String topPlace = (topDmdSec.getPlacePublication() != null) ? topDmdSec.getPlacePublication() : topDmdSec.getPlace();
		String topYear = (topDmdSec.getYearPublication() != null) ? topDmdSec.getYearPublication() : topDmdSec.getYear();
		String topVolume = topDmdSec.getVolume();
		String topIssueNo = topDmdSec.getIssueNo();
		
		for(Entry<String,DmdSec> dmdSecMap : dmdSecs.entrySet()) {

			MetsSolrRecord metsSolrRecord = new MetsSolrRecord();
			String dmdLogId = dmdSecMap.getKey();
			DmdSec dmdSec = dmdSecMap.getValue();
			StructMapLogical structMapLogical = structMapsLogical.get(dmdLogId);

			
			String childAcNo = dmdSec.getAcNo();
			String childAkIdentifier = dmdSec.getAkIdentifier();
			List<String> childClassifications = (!dmdSec.getClassifications().isEmpty()) ? dmdSec.getClassifications() : null;
			String childLanguageTerm = dmdSec.getLanguageTerm();
			String childPublisher = (dmdSec.getPublisherPublication() != null) ? dmdSec.getPublisherPublication() : dmdSec.getPublisher();
			String childPlace = (dmdSec.getPlacePublication() != null) ? dmdSec.getPlacePublication() : dmdSec.getPlace();
			String childYear = (dmdSec.getYearPublication() != null) ? dmdSec.getYearPublication() : dmdSec.getYear();
			String childVolume = dmdSec.getVolume();
			String childIssueNo = dmdSec.getIssueNo();
			String childTitle = dmdSec.getTitle();
			String childSubtitle = dmdSec.getSubTitle();
			List<String> childAbstracts = (!dmdSec.getAbstractTexts().isEmpty()) ? dmdSec.getAbstractTexts() : null;
			List<Participant> childParticipants = (!dmdSec.getParticipants().isEmpty()) ? dmdSec.getParticipants() : null;
			String childSortNo = dmdSec.getSortNo();
			
			
			
			
			
			// Transfor data that we already have in raw record to record for Solr
			//metsSolrRecord.setDmdLogId(dmdLogId);
			metsSolrRecord.setAbstractTexts(childAbstracts);
			metsSolrRecord.setAcNo((childAcNo != null) ? childAcNo : topAcNo);
			metsSolrRecord.setAkIdentifier((childAkIdentifier != null) ? childAkIdentifier : topAkIdentifier);
			metsSolrRecord.setClassifications((childClassifications != null) ? childClassifications : topClassifications);
			metsSolrRecord.setIssueNo((childIssueNo != null) ? childIssueNo : topIssueNo);
			metsSolrRecord.setLanguageTerm((childLanguageTerm != null) ? childLanguageTerm : topLanguageTerm);
			metsSolrRecord.setParticipants(childParticipants);
			metsSolrRecord.setPlace((childPlace != null) ? childPlace : topPlace);
			metsSolrRecord.setPublisher((childPublisher != null) ? childPublisher : topPublisher);
			metsSolrRecord.setSortNo(childSortNo);
			metsSolrRecord.setSubTitle(childSubtitle);
			metsSolrRecord.setTitle(childTitle);
			metsSolrRecord.setVolume((childVolume != null) ? childVolume : topVolume);
			metsSolrRecord.setYear((childYear != null) ? childYear : topYear);


			// Get entry in sturctMapsLogical for current dmdSec and set the values to record for Solr

			//metsSolrRecord.setLogId(structMapLogical.getLogId());
			String logId_structMapLogical = null;
			if (structMapLogical != null) {
				metsSolrRecord.setType(structMapLogical.getType());
				metsSolrRecord.setLevel(structMapLogical.getLevel());
				logId_structMapLogical = structMapLogical.getLogId();
			}

			// Get infos from structLink
			List<String> physIds = new ArrayList<String>();
			for (Entry<String,StructLink> structLinkMap : structLinks.entrySet()) {

				String logId_structLink = structLinkMap.getValue().getSmLinkFrom();
				if (logId_structLink == logId_structMapLogical) {
					physIds.add(structLinkMap.getValue().getSmLinkTo());
				}
			}

			/*
			metsSolrRecord.setOrder();
			metsSolrRecord.setOrderLabelFrom();
			metsSolrRecord.setOrderLabelTo();
			metsSolrRecord.setUrn();
			 */

			metsSolrRecords.add(metsSolrRecord);

		}

		return metsSolrRecords;
	}


	// Methods of ContentHandler that are not used at the moment
	@Override
	public void setDocumentLocator(Locator locator) {}

	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException {}

	@Override
	public void endPrefixMapping(String prefix) throws SAXException {}

	@Override
	public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}

	@Override
	public void processingInstruction(String target, String data) throws SAXException {}

	@Override
	public void skippedEntity(String name) throws SAXException {}



}
