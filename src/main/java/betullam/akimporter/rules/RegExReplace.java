package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class RegExReplace {

	public static List<String> getRegexReplaceValues(List<String> dataFieldValues, String dataRule) {
		List<String> regexReplaceValues = new ArrayList<String>();
		
		LinkedHashMap<Integer, String> bracketValues = AkImporterHelper.getBracketValues(dataRule);
		String regexReplacePattern = bracketValues.get(1);
		String regexReplaceValue = bracketValues.get(2);
		
		if (regexReplaceValue == null) {
			regexReplaceValue = "";
		}
		
		for (String dataFieldValue : dataFieldValues) {
			String returnValue = (dataFieldValue != null) ? dataFieldValue.replaceAll(regexReplacePattern, regexReplaceValue).trim() : null;
			if (returnValue != null && !returnValue.isEmpty()) {
				regexReplaceValues.add(returnValue);
			} else {
				regexReplaceValues.add(null);
			}
			
		}
		return regexReplaceValues;
	}
	
}