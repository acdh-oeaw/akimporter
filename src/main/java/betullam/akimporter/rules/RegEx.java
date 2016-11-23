package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class RegEx {
	
	public static List<String> getRegexValues(List<String> dataFieldValues, String dataRule) {
		List<String> regexValues = new ArrayList<String>();
		
		LinkedHashMap<Integer, String> bracketValues = AkImporterHelper.getBracketValues(dataRule);
		String regexPattern = bracketValues.get(1);
		
		for (String dataFieldValue : dataFieldValues) {
			Pattern pattern = java.util.regex.Pattern.compile(regexPattern);
			Matcher matcher = pattern.matcher(dataFieldValue);
			String returnValue = null;
			String regexedValue = "";
			while (matcher.find()) {
				regexedValue = regexedValue.concat(matcher.group());
			}
			if (!regexedValue.trim().isEmpty()) {
				returnValue = regexedValue.trim();
			} else {
				// Return original field value if regex does not match.
				// With regExStrict[PATTERN], we return null instead of the original field value.
				returnValue = dataFieldValue;
			}
			
			regexValues.add(returnValue);
		}
		return regexValues;
	}
}