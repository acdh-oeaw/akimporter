package main.java.betullam.akimporter.rules;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;

import ak.xmlhelper.XmlParser;

public class Rules {

	public static String oaiPropertiesFilePath;
	public static Document document;
	public static XmlParser xmlParser = new XmlParser();
	private static String[] dataRuleNames = new String[] {
			"multiValued",
			"customText",
			"translateValue",
			"translateConnectedSubfields",
			"translateConcatenatedSubfields",
			"translateValueContains",
			"translateValueRegex",
			"defaultValue",
			"regEx",
			"regExStrict",
			"regExReplace",
			"allowDuplicates",
			"connectedSubfields",
			"concatenatedSubfields",
			"subfieldExists",
			"subfieldNotExists",
			"applyToFields",
			"getAllFields",
			"getFullRecordAsXML"
	};


	public static List<String> applyDataRules(String solrField, List<String> dataFieldValues, List<String> dataRules) {

		List<String> treatedValues = new ArrayList<String>();

		if (dataRules != null && !dataRules.isEmpty()) {

			for (String dataRule : dataRules) {

				if (dataRule.equals("customText")) {
					treatedValues.addAll(CustomText.getCustomText(dataFieldValues));
				}

				if (dataRule.contains("translateValue")) {
					treatedValues.addAll(TranslateValue.getTranslatedValues(dataFieldValues, dataRule));
				}

				if (dataRule.contains("connectedSubfields")) {
					treatedValues.addAll(ConnectedFields.getConnectedFields(dataFieldValues, dataRule));
				}
			}

			// None of the rules above applied, so we fill the "treatedValues" List, that is still empty by now, with the original values
			if (treatedValues.isEmpty()) {
				treatedValues.addAll(dataFieldValues);
			}

			// The last rules to apply are the following
			if (!treatedValues.isEmpty()) {
				if (!dataRules.contains("multiValued")) {
					List<String> firstValue = MultiValued.getFirstValue(treatedValues);
					treatedValues.clear();
					treatedValues = firstValue;
				} else {
					if (!dataRules.contains("allowDuplicates")) {
						List<String> dedupValues = AllowDuplicates.getDeduplicatedList(treatedValues);
						treatedValues.clear();
						treatedValues = dedupValues;
					}
				}
			} else {
				treatedValues = null;
			}
		} else {
			// There are nor rules to apply, so return the original values
			treatedValues = dataFieldValues;
		}

		return treatedValues;
	}

	
	public static List<PropertyBag> getPropertyBags(String oaiPropertiesFile) {

		List<PropertyBag> propertyBags = new ArrayList<PropertyBag>();
		Map<String, String> propertiesAsMap = getPropertiesAsMap(oaiPropertiesFile);

		for (Entry<String, String> property : propertiesAsMap.entrySet()) {
			PropertyBag propertyBag = new PropertyBag();
			String solrFieldName = property.getKey();
			List<String> propertyValues = getPropertyValues(property.getValue());
			List<String> dataFields = getDataFields(propertyValues);
			List<String> dataRules = getDataRules(propertyValues);

			propertyBag.setSolrField(solrFieldName);
			propertyBag.setDataFields(dataFields);
			propertyBag.setDataRules(dataRules);

			propertyBags.add(propertyBag);
		}
		return propertyBags;
	}


	public static List<String> getDataFields(List<String> propertyValues) {
		List<String> dataFields = new ArrayList<String>();
		for (String propertyValue : propertyValues) {
			if (!StringUtils.startsWithAny(propertyValue, dataRuleNames)) {
				dataFields.add(propertyValue);
			}
		}		
		return dataFields;
	}


	public static List<String> getDataRules(List<String> propertyValues) {
		List<String> dataRules = new ArrayList<String>();
		for (String propertyValue : propertyValues) {
			if (StringUtils.startsWithAny(propertyValue, dataRuleNames)) {
				dataRules.add(propertyValue);
			}
		}		
		return dataRules;
	}




	private static List<String> getPropertyValues(String propertyValue) {
		List<String> propertyValues = new ArrayList<String>();

		String value = "";
		int bracketCounter = 0;

		// Iterate over each character of the property value (comma separated string)
		for (int i = 0; i < propertyValue.length(); i++) {
			char c = propertyValue.charAt(i);

			// The single character as String
			String s = Character.toString(c);

			// Add the single character to a String variable
			value += s;

			// Check if the current character is an opening bracket
			if (s.equals("[")) {
				bracketCounter = bracketCounter + 1;
			}

			// Check if the current character is a closing bracket
			if (s.equals("]")) {
				bracketCounter = bracketCounter - 1;
			}

			// Check if the current character is a comma and is not within brackets (to avoid the splitting of values at commas within RegEx expressions) or
			// if it is the last part of the property value (as this does not end with a comma, it would not be added to the List<String> without that check).
			if ((s.equals(",") && bracketCounter <= 0) || (i == (propertyValue.length()-1))) {
				// Add the property value Replace comma at the last position.
				propertyValues.add(value.trim().replaceAll(",$", ""));
				// Reset the variable for the next value.
				value = "";
			}
		}

		return propertyValues;
	}


	private static Map<String, String> getPropertiesAsMap(String oaiPropertiesFile) {
		Map<String, String> indexingProperties = null;

		// Load contents of propertiesAsMap-file:
		try {
			indexingProperties = new HashMap<String, String>();
			Properties properties = new Properties();
			BufferedInputStream propertiesStream = new BufferedInputStream(new FileInputStream(oaiPropertiesFile));
			properties.load(propertiesStream);
			propertiesStream.close();

			// Loop through propertiesAsMap and put them into a Map<String, String>:
			for(String key : properties.stringPropertyNames()) {
				indexingProperties.put(key, properties.getProperty(key));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return indexingProperties;
	}

	public static String getOaiPropertiesFilePath() {
		return oaiPropertiesFilePath;
	}

	public static void setOaiPropertiesFilePath(String oaiPropertiesFilePath) {
		Rules.oaiPropertiesFilePath = oaiPropertiesFilePath;
	}

	public static Document getDocument() {
		return document;
	}

	public static void setDocument(Document document) {
		Rules.document = document;
	}

}


/*
 * 
 * Terminology
 * ###########
 * 
 * 
 *	Properties:
 *	-----------
 *	Is all content of the .properties file
 *	
 *
 *	Property:
 *	---------
 *	Is 1 line of a the .properties file.
 *		Property Key:		Left part of the property. Represents the Solr field name.
 *		Property Value:		Right part of the property. A comma separated String containing all Fields and Rules. 
 *		Property Values:	All Fields and Rules contained within the Property Value as single elements, splitted at the commas.
 *
 *
 *	Property Bag:
 *	-------------
 *	A collection (custom class) that contains the Solr field name, all Data Fields and all Data Rules
 *
 *
 *	Data Fields:
 *	-------
 *	Are the instructions that tell us which values from which fields from our data should be parsed.
 *	They are part of the "Property Value" and can be MAB or MARC indications or xPath instructions.
 *	E. g.: 331$**$a, /record/metadata/dcvalue[@element='title']
 *	
 *
 *	Data Rules:
 *	------
 *	All rules for treating the values in our data fields that we want to index. These are all data rules of 1 line in the .properties file.
 *	They are part of the "Property Value".
 *	E. g.: regEx[PATTERN], multiValued, allowDuplicates
 *	
 *
 *	Data Rule:
 *	-----
 *	Is one single rule for treating the values in our data fields to index. In other words: a Data Rule is one single value from the Data Rules.
 *	E. g.: multiValued
 */
