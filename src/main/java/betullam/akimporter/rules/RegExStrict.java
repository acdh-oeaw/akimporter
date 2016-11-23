package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class RegExStrict {
	
	public static List<String> getRegexStrictValues(List<String> dataFieldValues, String dataRule) {
		List<String> regexStrictValues = new ArrayList<String>();
		
		LinkedHashMap<Integer, String> bracketValues = AkImporterHelper.getBracketValues(dataRule);
		String regexStrictPattern = bracketValues.get(1);
		
		for (String dataFieldValue : dataFieldValues) {
			Pattern pattern = java.util.regex.Pattern.compile(regexStrictPattern);
			Matcher matcher = pattern.matcher(dataFieldValue);
			String returnValue = null;
			String regexedStrictValue = "";
			while (matcher.find()) {
				regexedStrictValue = regexedStrictValue.concat(matcher.group());
			}
			if (!regexedStrictValue.trim().isEmpty()) {
				returnValue = regexedStrictValue.trim();
			} else {
				// Return null ("strict" regex)
				regexedStrictValue = null;
			}
			
			regexStrictValues.add(returnValue);
		}
		return regexStrictValues;
	}
}