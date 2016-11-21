package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;

import javax.xml.xpath.XPathExpressionException;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class ConnectedFields {


	public static List<String> getConnectedFields(List<String> dataFieldValues, String dataRule) {

		List<String> returnValue = new ArrayList<String>();
		
		for (String dataFieldValue : dataFieldValues) {
			// Add the main value to the list
			returnValue.add(dataFieldValue);
			
			String connectedDefaultValue = null;
			LinkedHashMap<Integer, String> connectedFields = AkImporterHelper.getBracketValues(dataRule);
			
			for (Entry<Integer, String> connectedField : connectedFields.entrySet()) {
				// TODO: Splitting with colon can be tricky because XML element names with namespaces have colons in them also.
				List<String> immutableList = Arrays.asList(connectedField.getValue().split("\\s*:\\s*"));
				List<String> connectedFieldsCodes = new ArrayList<String>();
				connectedFieldsCodes.addAll(immutableList); // Create CHANGEABLE/MUTABLE List
				int lastListElement = (connectedFieldsCodes.size()-1); // Get index of last List element
				connectedDefaultValue = connectedFieldsCodes.get(lastListElement); // Last value is always the default value to use
				connectedFieldsCodes.remove(lastListElement); // Remove the default value so that only the subfield codes will remain

				String textToUse = null;
				for (String connectedFieldsCode : connectedFieldsCodes) {
					try {
						List<String> connectedFieldValues = Rules.xmlParser.getXpathResult(Rules.document, connectedFieldsCode);
						if (connectedFieldValues != null && !connectedFieldValues.isEmpty()) {
							for (String connectedFieldValue : connectedFieldValues) {
								// Set text only if textToUse is not null. Otherwise we would overwrite a value that was added in a loop before.
								if (textToUse == null) {
									textToUse = connectedFieldValue;
								}
							}
						}
					} catch (XPathExpressionException e) {
						// Do nothing. If there is an error with the xPath query, the default value should be used.
					}
				}

				// Set default value if no other value was found
				if (textToUse == null) {
					textToUse = connectedDefaultValue;
				}

				/*
				// TODO: Handle translation of connected values: Here or maybe better in AN OWN CLASS!
				boolean isTranslateConnectedFields = relevantPropertiesObject.isTranslateConnectedFields();
				if (isTranslateConnectedFields) {
					
				}
				 */
				
				returnValue.add(textToUse);
			}
		}
		
		return returnValue;
	}
	
}
