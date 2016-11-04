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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.common.SolrInputDocument;
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

	private MetsRawRecord metsRawRecord = null;
	private LinkedHashMap<String, DmdSec> dmdSecs = null;
	private DmdSec dmdSec = null;
	private LinkedHashMap<String, StructMapLogical> structMapsLogical = null;
	private StructMapLogical structMapLogical = null;
	private LinkedHashMap<String, StructMapPhysical> structMapsPhysical = null;
	private StructMapPhysical structMapPhysical = null;
	private List<StructLink> structLinks = null;
	private StructLink structLink = null;
	private List<String> classifications = null;
	private Participant participant = null;
	private List<Participant> participants = null;

	private List<MetsSolrRecord> metsSolrRecords = null;

	public MetsContentHandler(SolrServer solrServer, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;
	}

	@Override
	public void startDocument() throws SAXException {
		metsSolrRecords = new ArrayList<MetsSolrRecord>();
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
			structLinks = new ArrayList<StructLink>();
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

			if (qName.equals("mods:classification")) {
				if (atts.getValue("authority") != null && atts.getValue("authority").equals("sswd")) {
					isClassification = true;
					/*
					for (int i = 0; i < atts.getLength(); i++) {
						System.out.println(atts.getQName(i) + ": " + atts.getValue(i));
					}
					*/
					
					
				} else {
					isClassification = false;
				}
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
					//smLinkTo = atts.getValue("xlink:to");
					structLink.setSmLinkTo(atts.getValue("xlink:to"));
				}/* else {
					smLinkTo = null;
				}*/

				if (atts.getValue("xlink:from") != null) {
					//smLinkFrom = atts.getValue("xlink:from");
					structLink.setSmLinkFrom(atts.getValue("xlink:from"));
				}/* else {
					smLinkFrom = null;
				}*/

				structLinks.add(structLink);

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
			metsSolrRecords.addAll(getMetsSolrRecords(metsRawRecord));
			System.out.println("End of record tag: " + metsSolrRecords.get(0).getClassifications());
			isRecord = false;
			
		}

		if (qName.equals("mets:dmdSec")) {
			dmdSec.setClassifications(classifications);
			dmdSec.setParticipants(participants);
			
			//classifications = null;
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
		//System.out.println(metsSolrRecords);
		
		solrAddRecordSet(solrServer, metsSolrRecords);
		System.out.println("endDocument(): " + metsSolrRecords.get(0).getClassifications());
	}


	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {	
		elementContent += new String(ch, start, length).replaceAll("\\s+", " ");
	}

	/**
	 * This method contains the code that actually adds a set of MetsSolrRecord objects
	 * (see MetsSolrRecord class) to the specified Solr server.
	 *
	 * @param solrServer			SolrServer: The Solr server to which the data should be indexed.
	 * @param metsSolrRecords		List<SolrRecord>: A list of SolrRecord objects that should be indexed.
	 */
	public void solrAddRecordSet(SolrServer solrServer, List<MetsSolrRecord> metsSolrRecords) {
		try {

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();


			List<String> parentVolumeFields = new ArrayList<String>();
			parentVolumeFields.add("allfields");
			parentVolumeFields.add("container_volume");

			List<String> parentIssueFields = new ArrayList<String>();
			parentIssueFields.add("allfields");
			parentIssueFields.add("container_issue");

			List<String> parentTitleFields = new ArrayList<String>();
			parentTitleFields.add("allfields");
			parentTitleFields.add("container_title");

			List<String> titleFields = new ArrayList<String>();
			titleFields.add("allfields");
			titleFields.add("title");
			titleFields.add("title_short");
			titleFields.add("title_full");
			titleFields.add("title_auth");

			List<String> subTitleFields = new ArrayList<String>();
			subTitleFields.add("allfields");
			subTitleFields.add("title_sub");

			List<String> authorFields = new ArrayList<String>();
			authorFields.add("allfields");
			authorFields.add("author");
			authorFields.add("author-letter");
			authorFields.add("author_sort");
			authorFields.add("author_fuller");
			//author_role

			List<String> author2Fields = new ArrayList<String>();
			author2Fields.add("allfields");
			author2Fields.add("author2");
			//author2_role

			List<String> authorAdditionalFields = new ArrayList<String>();
			authorAdditionalFields.add("allfields");
			authorAdditionalFields.add("author_additional");
			//author_additional_NameRoleGnd_str_mv

			List<String> classificationFields = new ArrayList<String>();
			classificationFields.add("allfields");
			classificationFields.add("topic");
			classificationFields.add("topic_facet");
			classificationFields.add("swdTopic_txt_mv");

			List<String> publisherFields = new ArrayList<String>();
			publisherFields.add("allfields");
			publisherFields.add("publisher");

			List<String> publishDateFields = new ArrayList<String>();
			publishDateFields.add("allfields");
			publishDateFields.add("publishDate");
			publishDateFields.add("publishDateSort");

			List<String> publishPlaceFields = new ArrayList<String>();
			publishPlaceFields.add("allfields");
			publishPlaceFields.add("publishPlace_txt");			

			List<String> urlFields = new ArrayList<String>();
			urlFields.add("allfields");
			urlFields.add("url");

			List<String> languageFields = new ArrayList<String>();
			languageFields.add("allfields");
			languageFields.add("language");

			List<String> abstractFields = new ArrayList<String>();
			abstractFields.add("allfields");
			abstractFields.add("abstract_txt");

			List<String> pageFromFields = new ArrayList<String>();
			pageFromFields.add("allfields");
			pageFromFields.add("orderLabelFrom_str");

			List<String> pageToFields = new ArrayList<String>();
			pageToFields.add("allfields");
			pageToFields.add("orderLabelTo_str");

			List<String> sortNumberFields = new ArrayList<String>();
			sortNumberFields.add("allfields");
			sortNumberFields.add("sortNo_str");
			sortNumberFields.add("order_str");

			List<String> levelFields = new ArrayList<String>();
			levelFields.add("allfields");
			levelFields.add("level_str");

			List<String> formatFields = new ArrayList<String>();
			formatFields.add("allfields");
			formatFields.add("format");

			Map<String,List<String>> indexFields = new LinkedHashMap<String,List<String>>();
			indexFields.put("classifications", classificationFields);
			indexFields.put("publisher", publisherFields);
			indexFields.put("place", publishPlaceFields);
			indexFields.put("year", publishDateFields);
			indexFields.put("volume", parentVolumeFields);
			indexFields.put("issueNo", parentIssueFields);
			indexFields.put("sortNo", sortNumberFields);
			indexFields.put("title", titleFields);
			indexFields.put("subTitle", subTitleFields);
			indexFields.put("languageTerm", languageFields);
			indexFields.put("abstractTexts", abstractFields);
			indexFields.put("type", formatFields);
			indexFields.put("level", levelFields);
			indexFields.put("urn", urlFields);
			indexFields.put("order", sortNumberFields);
			indexFields.put("orderLabelFrom", pageFromFields);
			indexFields.put("orderLabelTo", pageToFields);
			indexFields.put("author", authorFields);
			indexFields.put("author2", author2Fields);
			indexFields.put("authorAdditional", authorAdditionalFields);

			
			
			//System.out.println(metsSolrRecords);
			for (MetsSolrRecord metsSolrRecord : metsSolrRecords) {
				
				
				
				
				String id = metsSolrRecord.getUrn();
				
				if (id != null) {
					// Create a document:
					SolrInputDocument doc = new SolrInputDocument();
					doc.addField("id", metsSolrRecord.getUrn());
					String parentAcNoRaw = metsSolrRecord.getAcNo();
					if (parentAcNoRaw != null) {
						// Remove suffix of AC Nos, e. g. remove "_2016_1" from "AC08846807_2016_1"
						Pattern pattern = Pattern.compile("^[A-Za-z0-9]+");
						Matcher matcher = pattern.matcher(metsSolrRecord.getAcNo());
						String parentAc = (matcher.find()) ? matcher.group().trim() : null;
						doc.addField("parentAC_str_mv", metsSolrRecord.getAcNo());
					}
					doc.addField("recordtype", "Dependant"); // ???

					for (String solrFieldName : indexFields.get("classifications")) {
						
						doc.addField(solrFieldName, metsSolrRecord.getClassifications());
					}
					
					for (String solrFieldName : indexFields.get("publisher")) {
						doc.addField(solrFieldName, metsSolrRecord.getPublisher());
					}
					
					for (String solrFieldName : indexFields.get("place")) {
						doc.addField(solrFieldName, metsSolrRecord.getPlace());
					}
					
					for (String solrFieldName : indexFields.get("year")) {
						doc.addField(solrFieldName, metsSolrRecord.getYear());
					}
					
					for (String solrFieldName : indexFields.get("volume")) {
						doc.addField(solrFieldName, metsSolrRecord.getVolume());
					}
					
					for (String solrFieldName : indexFields.get("issueNo")) {
						doc.addField(solrFieldName, metsSolrRecord.getIssueNo());
					}
					
					for (String solrFieldName : indexFields.get("sortNo")) {
						doc.addField(solrFieldName, metsSolrRecord.getSortNo());
					}
					
					for (String solrFieldName : indexFields.get("title")) {
						doc.addField(solrFieldName, metsSolrRecord.getTitle());
					}
					
					for (String solrFieldName : indexFields.get("subTitle")) {
						doc.addField(solrFieldName, metsSolrRecord.getSubTitle());
					}
					/*
					for (Entry<String,List<String>> indexField : indexFields.entrySet()) {
						String fieldClass = indexField.getKey();
						List<String> solrFieldNames = indexField.getValue();
						
						if (fieldClass.equals("classifications")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getClassifications());
							}
						}
						
						if (fieldClass.equals("publisher")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getPublisher());
								System.out.println(metsSolrRecord.getPublisher());
							}
						}
						
						if (fieldClass.equals("place")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getPlace());
							}
						}
						
						if (fieldClass.equals("year")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getYear());
							}
						}
						
						if (fieldClass.equals("volume")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getVolume());
							}
						}
						
						if (fieldClass.equals("issueNo")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getIssueNo());
							}
						}
						
						if (fieldClass.equals("sortNo")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getSortNo());
							}
						}
						
						if (fieldClass.equals("title")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getTitle());
							}
						}
						
						if (fieldClass.equals("subTitle")) {
							for (String solrFieldName : solrFieldNames) {
								doc.addField(solrFieldName, metsSolrRecord.getSubTitle());
							}
						}
					}
					*/
					// Add the timestamp of indexing (it is the timstamp of the beginning of the indexing process):
					// TODO: Change hardecoded fieldname "indexTimestamp_str" to fieldname specified in .properties file.
					doc.addField("indexTimestamp_str", timeStamp);

					// Add the document to the collection of documents:
					docs.add(doc);
				}
				
			}


			
			if (!docs.isEmpty()) {
				// Now add the collection of documents to Solr:
				//solrServer.add(docs);
				//solrServer.commit();
				// Set "docs" to "null" (save memory):
				docs = null;
			}


		}/* catch (SolrServerException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}*/ catch (Exception e) {
			e.printStackTrace();
		}
		
		
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

	private List<MetsSolrRecord> getMetsSolrRecords(MetsRawRecord metsRawRecord) {

		// Return variable
		List<MetsSolrRecord> metsSolrRecords = new ArrayList<MetsSolrRecord>();

		// Get all subclasses from raw mets record
		LinkedHashMap<String,DmdSec> dmdSecs = metsRawRecord.getDmdSecs();
		LinkedHashMap<String,StructMapLogical> structMapsLogical = metsRawRecord.getStructMapsLogical();
		LinkedHashMap<String,StructMapPhysical> structMapsPhysical = metsRawRecord.getStructMapsPhysical();
		List<StructLink> structLinks = metsRawRecord.getStructLinks();

		// Loop over logical StructMaps and get all data that are relevant for indexing to Solr
		String topDmdLogId = null;
		if (structMapsLogical != null) {

			DmdSec topDmdSec = null;
			String topAcNo = null;
			String topAkIdentifier = null;
			List<String> topClassifications = null;
			String topLanguageTerm = null;
			String topPublisher = null;
			String topPlace = null;
			String topYear = null;
			String topVolume = null;
			String topIssueNo = null;


			for (Entry<String,StructMapLogical> structMapLogicalMap : structMapsLogical.entrySet()) {
				MetsSolrRecord metsSolrRecord = new MetsSolrRecord();
				StructMapLogical structMapLogical = structMapLogicalMap.getValue();

				String dmdLogId = structMapLogical.getDmdLogId(); // Only DMDLOG-ID or null
				String logId = structMapLogical.getLogId(); // Only LOG-ID or null

				// Get the StructMapLogical that has a DMD-ID and also has the lowest level. This is the topmost element that contains data
				// like publication year and place, volume no., issue no., etc. that are relevant for all other records like articles or chapters.
				if (structMapLogical.getDmdLogId() != null && topDmdLogId == null) {
					topDmdLogId = structMapLogical.getDmdLogId();
					topDmdSec = dmdSecs.get(topDmdLogId);
					topAcNo = topDmdSec.getAcNo();
					topAkIdentifier = topDmdSec.getAkIdentifier();
					topClassifications = topDmdSec.getClassifications();
					topLanguageTerm = topDmdSec.getLanguageTerm();
					topPublisher = (topDmdSec.getPublisherPublication() != null) ? topDmdSec.getPublisherPublication() : topDmdSec.getPublisher();
					topPlace = (topDmdSec.getPlacePublication() != null) ? topDmdSec.getPlacePublication() : topDmdSec.getPlace();
					topYear = (topDmdSec.getYearPublication() != null) ? topDmdSec.getYearPublication() : topDmdSec.getYear();
					topVolume = topDmdSec.getVolume();
					topIssueNo = topDmdSec.getIssueNo();
				}
				//System.out.println(topClassifications);
				

				// Get the metadata from the metadata section (dmdSec)
				DmdSec dmdSec = (topDmdLogId != null && !topDmdLogId.equals(dmdSecs.get(dmdLogId))) ? dmdSecs.get(dmdLogId) : null;
				
				if (dmdSec != null) {
					/*
					String childAcNo = null;
					String childAkIdentifier = null;
					List<String> childClassifications = null;
					String childLanguageTerm = null;
					String childPublisher = null;
					String childPlace = null;
					String childYear = null;
					String childVolume = null;
					String childIssueNo = null;
					String childTitle = null;
					String childSubtitle = null;
					List<String> childAbstracts = null;
					List<Participant> childParticipants = null;
					String childSortNo = null;
					*/
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
					
					// Set metadata to record for Solr
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
					
					//System.out.println(childClassifications);
				}

				

				// Set some logical data:
				metsSolrRecord.setLevel(structMapLogical.getLevel());
				metsSolrRecord.setType(structMapLogical.getType());

				// Get data from StructLinks
				List<String> physIds = new ArrayList<String>();
				for (StructLink structLink : structLinks) {
					String logId_structLink = structLink.getSmLinkFrom();
					String physId_structLink = structLink.getSmLinkTo();
					if (logId.equals(logId_structLink)) {
						physIds.add(physId_structLink);
					}
				}

				// With physIds from StructLinks, get data from physical StructMap
				if (!physIds.isEmpty()) {
					String physIdFirst = physIds.get(0); // This represents the first page of an article, chapter, ...
					String physIdLast = physIds.get(physIds.size()-1); // This represents the last page of an article, chapter, ...

					String urn = structMapsPhysical.get(physIdFirst).getContentId();
					int order = structMapsPhysical.get(physIdFirst).getOrder();
					String orderLabelFirst = structMapsPhysical.get(physIdFirst).getOrderLabel();
					String orderLabelLast = structMapsPhysical.get(physIdLast).getOrderLabel();
					metsSolrRecord.setOrder(order);
					metsSolrRecord.setOrderLabelFrom(orderLabelFirst);
					metsSolrRecord.setOrderLabelTo(orderLabelLast);
					metsSolrRecord.setUrn(urn);
				}
				
				metsSolrRecords.add(metsSolrRecord);
			}
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
