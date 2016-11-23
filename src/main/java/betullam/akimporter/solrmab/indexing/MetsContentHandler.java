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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import main.java.betullam.akimporter.main.AkImporterHelper;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.DmdSec;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.Participant;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.StructLink;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.StructMapLogical;
import main.java.betullam.akimporter.solrmab.indexing.MetsRawRecord.StructMapPhysical;
import main.java.betullam.akimporter.solrmab.relations.RelationHelper;

public class MetsContentHandler implements ContentHandler {

	private HttpSolrServer solrServer;
	List<String> structElements;
	private String timeStamp;
	private boolean print = true;
	RelationHelper relationHelper = null;
	
	private String elementContent;

	private boolean isRecord = false;
	private boolean isDmdSec = false;
	private boolean isStructMapLogical = false;
	private boolean isStructMapPhysical = false;
	private boolean isStructLink = false;
	private boolean isClassification = false;
	private boolean isSswd = false;
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

	private String dmdLogIdDmdSec = null;
	private String dmdLogId_logicalStructMap = null;
	private String logId_logicalStructMap = null;
	private String contentId_logicalStructMap = null;
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
	
	private HashMap<String, String> translateProperties = null;

	
	// TODO: Do not translate hard-coded! Think about using .properties or .ini files!
	// Translation files


	public MetsContentHandler(HttpSolrServer solrServer, List<String> structElements, String timeStamp, boolean print) {
		this.solrServer = solrServer;
		this.structElements = structElements;
		this.timeStamp = timeStamp;
		this.print = print;
		this.relationHelper = new RelationHelper(solrServer, null, timeStamp);
		this.translateProperties = AkImporterHelper.getTranslateProperties("roles.properties", null, true);
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
				isStructMapLogical = true;
				level = 0;
			}

			if (qName.equals("mets:structMap") && atts.getValue("TYPE") != null && atts.getValue("TYPE").equals("PHYSICAL")) {
				isStructMapPhysical = true;
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
				isClassification = true;
				if (atts.getValue("authority") != null && atts.getValue("authority").equals("sswd")) {
					isSswd = true;
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
					dmdSec.setSortNo(atts.getValue("order"));
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


		if (isStructMapLogical) {

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

				if (atts.getValue("CONTENTIDS") != null) {
					contentId_logicalStructMap = atts.getValue("CONTENTIDS");
					structMapLogical.setContentId(contentId_logicalStructMap);
				} else {
					contentId_logicalStructMap = null;
				}

				if (atts.getValue("TYPE") != null) {
					structMapLogical.setType(atts.getValue("TYPE"));
				}

				if (atts.getValue("LABEL") != null) {
					structMapLogical.setLabel(atts.getValue("LABEL"));
				}

				if (dmdLogId_logicalStructMap != null) {
					structMapsLogical.put(dmdLogId_logicalStructMap, structMapLogical);
				} else {
					structMapsLogical.put(logId_logicalStructMap, structMapLogical);
				}
			}
		}


		if (isStructMapPhysical) {
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
			}
		}


		if (isStructLink) {
			if (qName.equals("mets:smLink")) {
				structLink = metsRawRecord.new StructLink();

				if (atts.getValue("xlink:to") != null) {
					structLink.setSmLinkTo(atts.getValue("xlink:to"));
				}

				if (atts.getValue("xlink:from") != null) {
					structLink.setSmLinkFrom(atts.getValue("xlink:from"));
				}

				structLinks.add(structLink);
			}
		}

	}


	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {

		if (isClassification && isSswd) {
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
			String translatedRoleTerm = this.translateProperties.get(elementContent);
			String roleTerm = (translatedRoleTerm != null) ? translatedRoleTerm : elementContent;
			participant.setRole(roleTerm);
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
			metsSolrRecords.addAll(getMetsSolrRecords(metsRawRecord));
			isRecord = false;
		}

		if (qName.equals("mets:dmdSec")) {
			dmdSec.setClassifications(classifications);
			dmdSec.setParticipants(participants);
			dmdSecs.put(dmdLogIdDmdSec, dmdSec);
			isDmdSec = false;
		}

		if (qName.equals("mets:structMap")) {
			isStructMapLogical = false;
			isStructMapPhysical = false;
		}

		if (qName.equals("mets:div")) {
			if (isStructMapLogical) {
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
			isSswd = false;
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
		print(print, "\nIndexing documents to Solr ... ");
		solrAddRecordSet(solrServer, metsSolrRecords);
		print(print, "Done");
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

			// TODO: Add more data:
			// corporateAuthor...
			// deleted_str
			// dewey-???
			// fullrecord
			// fulltext
			// isbn
			// issn
			// first_indexed
			// last_indexed

			// Create a collection of all documents:
			Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

			List<String> parentTitleFields = new ArrayList<String>();
			parentTitleFields.add("allfields");
			parentTitleFields.add("articleParentTitle_txt");
			parentTitleFields.add("parentTitle_str_mv");

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

			List<String> author2Fields = new ArrayList<String>();
			author2Fields.add("allfields");
			author2Fields.add("author2");

			List<String> authorAdditionalFields = new ArrayList<String>();
			authorAdditionalFields.add("allfields");
			authorAdditionalFields.add("author_additional");

			List<String> classificationFields = new ArrayList<String>();
			classificationFields.add("allfields");
			classificationFields.add("topic");
			classificationFields.add("topic_facet");
			classificationFields.add("swdTopic_txt_mv");

			List<String> abstractFields = new ArrayList<String>();
			abstractFields.add("allfields");
			abstractFields.add("abstract_txt_mv");

			List<String> publisherFields = new ArrayList<String>();
			publisherFields.add("allfields");
			publisherFields.add("publisher");

			List<String> publishDateFields = new ArrayList<String>();
			publishDateFields.add("publishDate");
			publishDateFields.add("publishDateSort");
			publishDateFields.add("articleParentYear_str");

			List<String> publishPlaceFields = new ArrayList<String>();
			publishPlaceFields.add("allfields");
			publishPlaceFields.add("publishPlace_txt");			

			Map<String,List<String>> indexFields = new LinkedHashMap<String,List<String>>();
			indexFields.put("classificationFields", classificationFields);
			indexFields.put("publisherFields", publisherFields);
			indexFields.put("publishPlaceFields", publishPlaceFields);
			indexFields.put("publishDateFields", publishDateFields);
			indexFields.put("parentTitleFields", parentTitleFields);
			indexFields.put("titleFields", titleFields);
			indexFields.put("subTitleFields", subTitleFields);
			indexFields.put("abstractFields", abstractFields);
			indexFields.put("authorFields", authorFields);
			indexFields.put("author2Fields", author2Fields);
			indexFields.put("authorAdditionalFields", authorAdditionalFields);


			for (MetsSolrRecord metsSolrRecord : metsSolrRecords) {

				if (structElements.contains(metsSolrRecord.getType())) {
					// All
					String urlText = "Volltext";
					String urlPrefix = "https://emedien.arbeiterkammer.at/viewer/resolver?urn=";
					
					// Parent of parent data
					boolean isParentOfParent = metsSolrRecord.isParentOfParent();
					String parentOfParentUrn = metsSolrRecord.getParentOfParentUrn();
					String parentOfParentTitle = metsSolrRecord.getParentOfParentTitle();
					String parentOfParentType = metsSolrRecord.getParentOfParentType();
					int parentOfParentLevel = metsSolrRecord.getParentOfParentLevel();
					
					// Parent data
					boolean isParent = metsSolrRecord.isParent();
					String parentId = metsSolrRecord.getParentId();
					String parentUrn = metsSolrRecord.getParentUrn();
					String parentAcNo = metsSolrRecord.getAcNo();
					String parentTitle = metsSolrRecord.getParentTitle();
					String parentSubtitle = metsSolrRecord.getParentSubtitle();
					String parentPublisher = metsSolrRecord.getPublisher();
					String parentPlace = metsSolrRecord.getPlace();
					String parentPublicationYear = metsSolrRecord.getYear();
					String parentVolumeNo = metsSolrRecord.getVolume();
					String parentIssueNo = metsSolrRecord.getIssueNo();
					String parentSortNo = metsSolrRecord.getSortNo();
					String parentType = metsSolrRecord.getParentType();
					int parentLevel = metsSolrRecord.getParentLevel();
					
					// Child
					boolean isChild = metsSolrRecord.isChild();
					String childUrn = metsSolrRecord.getUrn();
					String childTitle = metsSolrRecord.getTitle();
					String childSubtitle = metsSolrRecord.getSubTitle();
					String childPageFrom = metsSolrRecord.getOrderLabelFrom();
					String childPageTo = metsSolrRecord.getOrderLabelTo();
					int childSortNo = metsSolrRecord.getOrder();
					List<String> childAbstractTexts = metsSolrRecord.getAbstractTexts();
					String childStructType = metsSolrRecord.getType();
					int childLevel = metsSolrRecord.getLevel();
					
					// Child and parent
					List<Participant> participants = metsSolrRecord.getParticipants();
					String language = metsSolrRecord.getLanguageTerm();
					List<String> classifications = metsSolrRecord.getClassifications();
					
					if (childUrn != null) {

						// Create a Solr input document:
						SolrInputDocument doc = new SolrInputDocument();
						
						// All
						doc.addField("recordtype", "mab");
						doc.addField("locationCode_str_mv", "goobi");
						doc.addField("location_txtF_mv", "eMedien");
						doc.addField("contentType_str_mv", "txt");
						doc.addField("format", "electronic");
						doc.addField("urlText_txtF_mv", urlText);
						doc.addField("indexTimestamp_str", timeStamp);

						// Parent of parent
						if (isParentOfParent) {
							doc.addField("id", parentOfParentUrn);
							for (String solrFieldName : indexFields.get("titleFields")) {
								doc.addField(solrFieldName, parentOfParentTitle);
							}
							doc.addField("url", urlPrefix + parentOfParentUrn);
							doc.addField("url", urlText);
							doc.addField("url", "NoMimeType");
							doc.addField("structType_str", parentOfParentType);
							doc.addField("level_str", parentOfParentLevel);
							
							// TODO: Add erscheinungsform_str dependent on StructType 
							// doc.addField("erscheinungsform_str", "Zeitschrift/Serie/...");
						}

						
						// Child and parent, but not parent of parent
						if (isChild || isParent) {
							for (String solrFieldName : indexFields.get("publisherFields")) {
								doc.addField(solrFieldName, parentPublisher);
							}
							for (String solrFieldName : indexFields.get("publishDateFields")) {
								doc.addField(solrFieldName, parentPublicationYear);
							}
							for (String solrFieldName : indexFields.get("publishPlaceFields")) {
								doc.addField(solrFieldName, parentPlace);
							}
							
							if (participants != null && !participants.isEmpty()) {
								Participant participant1 = participants.get(0);
								String participant1Role = participant1.getRole();
								String participant1AuthId = participant1.getAuthorityId();

								// author
								for (String solrFieldName : indexFields.get("authorFields")) {
									String givenName = participant1.getGivenName();
									String familyName = participant1.getFamilyName();
									String name = familyName;
									name += (givenName != null && !givenName.isEmpty()) ? ", " + givenName : "";
									doc.addField(solrFieldName, name);
								}
								doc.addField("author_role", participant1Role);
								doc.addField("author_GndNo_str", participant1AuthId);

								//author2
								if (participants.size() >= 2) {
									Participant participant2 = participants.get(1);

									String participant2Role = participant2.getRole();
									String participant2AuthId = participant2.getAuthorityId();

									for (String solrFieldName : indexFields.get("author2Fields")) {
										String givenName = participant2.getGivenName();
										String familyName = participant2.getFamilyName();
										String name = familyName;
										name += (givenName != null && !givenName.isEmpty()) ? ", " + givenName : "";
										doc.addField(solrFieldName, name);
									}
									doc.addField("author2_role", participant2Role);
									doc.addField("author2_GndNo_str", participant2AuthId);

									//authorAdditional
									if (participants.size() > 2) {
										for (Participant participant : participants) {
											if (!participant.equals(participant1) && !participant.equals(participant2)) { // We already have author 1 and 2, so treat only the other ones
												String givenName = participant.getGivenName();
												String familyName = participant.getFamilyName();
												String authorityId = (participant.getAuthorityId() != null && !participant.getAuthorityId().isEmpty()) ? participant.getAuthorityId() : "NoGndId";
												String role = (participant.getRole() != null && !participant.getRole().isEmpty()) ? participant.getRole() : "NoRole";

												String name = familyName;
												name += (givenName != null && !givenName.isEmpty()) ? ", " + givenName : "";

												for (String solrFieldName : indexFields.get("authorAdditionalFields")) {
													doc.addField(solrFieldName, name);
												}

												doc.addField("author_additional_NameRoleGnd_str_mv", name);
												doc.addField("author_additional_NameRoleGnd_str_mv", role);
												doc.addField("author_additional_NameRoleGnd_str_mv", authorityId);
												doc.addField("author_additional_GndNo_str_mv", (!authorityId.equals("NoGndId") ? authorityId : null));
											}
										}
									}
								}
							}
							
							for (String solrFieldName : indexFields.get("classificationFields")) {
								// ATTENTION: It's not possible to add a List<String> directly, e. g.: doc.addField(solrFieldName, classificationList);
								// It seems to be a bug?!?! That's why we have to iterate over the List and add the values one by one.
								for (String classification : classifications) {
									doc.addField(solrFieldName, classification);
								}
							}
						}
						
						
						// Parent
						if (isParent) {

							doc.addField("parentSYS_str_mv", parentOfParentUrn);
							doc.addField("parentTitle_str_mv", parentOfParentTitle);
							doc.addField("parentStructType_str", parentOfParentType);
							doc.addField("id", parentUrn);
							doc.addField("acNo_txt", parentAcNo);
							for (String solrFieldName : indexFields.get("titleFields")) {
								doc.addField(solrFieldName, parentTitle);
							}
							for (String solrFieldName : indexFields.get("subTitleFields")) {
								doc.addField(solrFieldName, parentSubtitle);
							}
							doc.addField("url", urlPrefix + parentUrn);
							doc.addField("url", urlText);
							doc.addField("url", "NoMimeType");
							doc.addField("structType_str", parentType);
							doc.addField("level_str", parentLevel);
							doc.addField("sortNo_str", parentSortNo);
							
							
							// TODO: Add:
							//	erscheinungsform_str (dependent on StructType)
							//	parentVolumeNo
							//	parentIssueNo

						}

						// Child
						if (isChild) {

							// TODO: Add parentOfParentSys when relating child to parent records.
							//doc.addField("parentOfParentSYS_str_mv", parentOfParentUrn);
							doc.addField("parentOfParentTitle_txt", parentOfParentTitle);
							doc.addField("parentOfParentStructType_str", parentOfParentType);
							if (metsSolrRecord.getParentOfParentLevel() > 0) {
								doc.addField("parentOfParentLevel_str", parentOfParentLevel);
							}

							// Fields "parentSYS_str_mv" and "parentTitle_str_mv" are responsible for displaying data in AKsearch
							doc.addField("parentSYS_str_mv", parentId);
							doc.addField("parentTitle_str_mv", parentOfParentTitle);
							
							doc.addField("parentAC_str_mv", parentAcNo);
							doc.addField("articleParentAC_str", parentAcNo);
							doc.addField("articleParentTitle_txt", parentOfParentTitle);
							doc.addField("articleParentSubtitle_txt", parentSubtitle);
							doc.addField("articleParentVolumeNo_str", parentVolumeNo);
							doc.addField("articleParentIssue_str", parentIssueNo);
							doc.addField("parentStructType_str", parentType);
							doc.addField("parentLevel_str", parentLevel);
							
							doc.addField("id", childUrn);
							doc.addField("sysNo_txt", childUrn);
							for (String solrFieldName : indexFields.get("titleFields")) {
								doc.addField(solrFieldName, childTitle);
							}
							for (String solrFieldName : indexFields.get("subTitleFields")) {
								doc.addField(solrFieldName, childSubtitle);
							}
							for (String solrFieldName : indexFields.get("abstractFields")) {
								// ATTENTION: It's not possible to add a List<String> directly, e. g.: doc.addField(solrFieldName, abstractsList);
								// It seems to be a bug?!?! That's why we have to iterate over the List and add the values one by one.
								if (childAbstractTexts != null && !childAbstractTexts.isEmpty() ) {
									for (String abstractText : childAbstractTexts) {
										doc.addField(solrFieldName, abstractText);
									}
								}
							}
							doc.addField("url", urlPrefix + childUrn);
							doc.addField("url", urlText);
							doc.addField("url", "NoMimeType");
							doc.addField("language", language);
							doc.addField("begrenzteWerke_str", "a|||||||");
							doc.addField("erscheinungsform_str", "Unselbständig");
							doc.addField("pageFrom_str", childPageFrom);
							doc.addField("pageTo_str", childPageTo);
							doc.addField("sortNo_str", childSortNo);
							doc.addField("structType_str", childStructType);
							doc.addField("level_str", childLevel);
						}

						// Add the document to the collection of documents:
						docs.add(doc);
					}
				}

			}

			if (!docs.isEmpty()) {
				// Now add the collection of documents to Solr:
				solrServer.add(docs);
				//solrServer.commit();
				// Set "docs" to "null" (save memory):
				docs = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	


	private List<MetsSolrRecord> getMetsSolrRecords(MetsRawRecord metsRawRecord) {
		// Return variable
		List<MetsSolrRecord> metsSolrRecords = new ArrayList<MetsSolrRecord>();

		// Get all subclasses from raw mets record
		LinkedHashMap<String,DmdSec> dmdSecs = metsRawRecord.getDmdSecs();
		LinkedHashMap<String,StructMapLogical> structMapsLogical = metsRawRecord.getStructMapsLogical();
		LinkedHashMap<String,StructMapPhysical> structMapsPhysical = metsRawRecord.getStructMapsPhysical();
		List<StructLink> structLinks = metsRawRecord.getStructLinks();

		// Loop over logical StructMaps and get all data that are relevant for indexing to Solr
		String parentDmdLogId = null;
		if (structMapsLogical != null) {

			//boolean isParentOfParent = false;
			String parentOfParentContentId = null;
			String parentOfParentLabel = null;
			String parentOfParentType = null;
			int parentOfParentLevel = 0;

			//boolean isParent = false;
			DmdSec parentDmdSec = null;
			String parentAcNoRaw = null;
			String parentAcNo = null;
			String parentId = null;
			String parentAkIdentifier = null;
			List<String> parentClassifications = null;
			String parentLanguageTerm = null;
			String parentPublisher = null;
			String parentPlace = null;
			String parentYear = null;
			String parentVolume = null;
			String parentIssueNo = null;
			String parentUrn = null;
			String parentTitle = null;
			String parentSubtitle = null;
			List<Participant> parentParticipants = null;
			int parentLevel = 0;

			//boolean isChild = false;
			String childAcNoRaw = null;
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

			for (Entry<String,StructMapLogical> structMapLogicalMap : structMapsLogical.entrySet()) {

				MetsSolrRecord metsSolrRecord = new MetsSolrRecord();
				StructMapLogical structMapLogical = structMapLogicalMap.getValue();

				String dmdLogId = structMapLogical.getDmdLogId();
				String logId = structMapLogical.getLogId();

				// Get the StructMapLogical that has a DMD-ID and also has the lowest level. This is the topmost element that contains data
				// like publication year and place, volume no., issue no., etc. that are relevant for all other records like articles or chapters.
				if (structMapLogical.getDmdLogId() != null && parentDmdLogId == null) {
					parentDmdLogId = structMapLogical.getDmdLogId();
					parentDmdSec = dmdSecs.get(parentDmdLogId);
					parentAcNoRaw = parentDmdSec.getAcNo();
					parentAkIdentifier = parentDmdSec.getAkIdentifier();
					parentClassifications = parentDmdSec.getClassifications();
					parentLanguageTerm = parentDmdSec.getLanguageTerm();
					parentPublisher = (parentDmdSec.getPublisherPublication() != null) ? parentDmdSec.getPublisherPublication() : parentDmdSec.getPublisher();
					parentPlace = (parentDmdSec.getPlacePublication() != null) ? parentDmdSec.getPlacePublication() : parentDmdSec.getPlace();
					parentYear = (parentDmdSec.getYearPublication() != null) ? parentDmdSec.getYearPublication() : parentDmdSec.getYear();
					parentVolume = parentDmdSec.getVolume();
					parentIssueNo = parentDmdSec.getIssueNo();
					parentTitle = parentDmdSec.getTitle();
					parentSubtitle = parentDmdSec.getSubTitle();
					parentParticipants = (!parentDmdSec.getParticipants().isEmpty()) ? parentDmdSec.getParticipants() : null;
					parentUrn = structMapLogical.getContentId();
					parentLevel = structMapLogical.getLevel();
					
					// Check if Solr record for the parent already exists so that we can relate the child
					// records to it:
					if (parentAcNoRaw != null) {
						// Remove suffix of AC Nos, e. g. remove "_2016_1" from "AC08846807_2016_1"
						Pattern pattern = Pattern.compile("^[A-Za-z0-9]+");
						Matcher matcher = pattern.matcher(parentAcNoRaw);
						parentAcNo = (matcher.find()) ? matcher.group().trim() : null;
					}
					
					if (parentAcNo != null) {
						SolrDocument parentRecord = relationHelper.getParentRecord(parentAcNo);
						parentId = (parentRecord != null && parentRecord.get("id") != null) ? parentRecord.get("id").toString() : null;
					}
				}

				
				// Get the metadata from the metadata section (dmdSec)
				DmdSec dmdSec = dmdSecs.get(dmdLogId);
				String urn = null;

				if (dmdSec != null) {

					if (dmdLogId.equals(parentDmdLogId)) {
						urn = parentUrn;
					} else {
						urn = null;
					}
					childAcNoRaw = dmdSec.getAcNo();
					if (childAcNoRaw != null) {
						// Remove suffix of AC Nos, e. g. remove "_2016_1" from "AC08846807_2016_1"
						Pattern pattern = Pattern.compile("^[A-Za-z0-9]+");
						Matcher matcher = pattern.matcher(childAcNoRaw);
						childAcNo = (matcher.find()) ? matcher.group().trim() : null;
					}
					childAkIdentifier = dmdSec.getAkIdentifier();
					childClassifications = (!dmdSec.getClassifications().isEmpty()) ? dmdSec.getClassifications() : null;
					childLanguageTerm = dmdSec.getLanguageTerm();
					childPublisher = (dmdSec.getPublisherPublication() != null) ? dmdSec.getPublisherPublication() : dmdSec.getPublisher();
					childPlace = (dmdSec.getPlacePublication() != null) ? dmdSec.getPlacePublication() : dmdSec.getPlace();
					childYear = (dmdSec.getYearPublication() != null) ? dmdSec.getYearPublication() : dmdSec.getYear();
					childVolume = dmdSec.getVolume();
					childIssueNo = dmdSec.getIssueNo();
					childTitle = dmdSec.getTitle();
					childSubtitle = dmdSec.getSubTitle();
					childAbstracts = (!dmdSec.getAbstractTexts().isEmpty()) ? dmdSec.getAbstractTexts() : null;
					childParticipants = (!dmdSec.getParticipants().isEmpty()) ? dmdSec.getParticipants() : null;
					childSortNo = dmdSec.getSortNo();

					// Set metadata to record for Solr
					metsSolrRecord.setParentId(parentId);
					metsSolrRecord.setAcNo((childAcNoRaw != null) ? childAcNo : parentAcNo);
					metsSolrRecord.setAkIdentifier((childAkIdentifier != null) ? childAkIdentifier : parentAkIdentifier);
					metsSolrRecord.setTitle(childTitle);
					metsSolrRecord.setSubTitle(childSubtitle);
					metsSolrRecord.setParticipants((childParticipants != null) ? childParticipants : parentParticipants);
					metsSolrRecord.setPublisher((childPublisher != null) ? childPublisher : parentPublisher);
					metsSolrRecord.setPlace((childPlace != null) ? childPlace : parentPlace);
					metsSolrRecord.setYear((childYear != null) ? childYear : parentYear);
					metsSolrRecord.setVolume((childVolume != null) ? childVolume : parentVolume);
					metsSolrRecord.setIssueNo((childIssueNo != null) ? childIssueNo : parentIssueNo);
					metsSolrRecord.setClassifications((childClassifications != null) ? childClassifications : parentClassifications);
					metsSolrRecord.setAbstractTexts(childAbstracts);
					metsSolrRecord.setLanguageTerm((childLanguageTerm != null) ? childLanguageTerm : parentLanguageTerm);
					metsSolrRecord.setSortNo(childSortNo);
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

					urn = (urn == null) ? structMapsPhysical.get(physIdFirst).getContentId() : urn; // Set URN if we don't have already one. If yes, it's the URN of the parent element.
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

			// Check the first StructMapLogical if it has no DMD-ID and a level lower than the top StructMapLogical. If yes,
			// we have the parent of the parent (e. g. a Periodical [= parent of the parent] of a PeriodicalVolume [= parent/top])
			Entry<String,MetsRawRecord.StructMapLogical> possibleParentOfParentMap = structMapsLogical.entrySet().iterator().next(); // Get first StructMapLogical in the Map
			StructMapLogical possibleParentOfParent = possibleParentOfParentMap.getValue();
			if (possibleParentOfParent.getDmdLogId() == null && possibleParentOfParent.getLevel() < parentLevel) {			
				parentOfParentContentId = possibleParentOfParent.getContentId();
				parentOfParentLabel = possibleParentOfParent.getLabel();
				parentOfParentLevel = possibleParentOfParent.getLevel();
				parentOfParentType = possibleParentOfParent.getType();
			}

			// Set all parent (e. g. Monograph, PeriodicalVolume) and parent of the parent (e. g. Periodical) metadata to all child records (e. g. Article, Chapter, ...):
			for (MetsSolrRecord metsSolrRecord : metsSolrRecords) {
				// Check if it is a parent of a parent, a parant or a child record record:
				int currentLevel = metsSolrRecord.getLevel();
				if (currentLevel == parentOfParentLevel) {
					metsSolrRecord.setIsParentOfParent(true);
				}
				if (currentLevel == parentLevel) {
					metsSolrRecord.setIsParent(true);
				}
				if (currentLevel > parentLevel) {
					metsSolrRecord.setIsChild(true);
				}
				metsSolrRecord.setParentOfParentTitle(parentOfParentLabel);
				metsSolrRecord.setParentOfParentUrn(parentOfParentContentId);
				metsSolrRecord.setParentOfParentType(parentOfParentType);
				metsSolrRecord.setParentOfParentLevel(parentOfParentLevel);
				metsSolrRecord.setParentTitle(parentTitle);
				metsSolrRecord.setParentSubtitle(parentSubtitle);
				metsSolrRecord.setParentUrn(parentUrn);
				metsSolrRecord.setParentLevel(parentLevel);
			}
		}

		//System.out.println(metsSolrRecords);
		return metsSolrRecords;
	}


	/**
	 * Prints the specified text to the console if "print" is true.
	 * 
	 * @param print		boolean: true if the text should be print, false otherwise
	 * @param text		String: a text message to print.
	 */
	private void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
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