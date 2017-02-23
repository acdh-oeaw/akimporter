package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class TranslateValue {

	
	public static List<String> getTranslatedValues(List<String> dataFieldValues, String dataRule) {
		List<String> translatedValues = null;
		String translateFileName = getTranslateFileName(dataRule);
		if (translateFileName != null) {
			translatedValues = new ArrayList<String>();
			Map<String, String> translateProperties = AkImporterHelper.getTranslateProperties(translateFileName, Rules.getOaiPropertiesFilePath(), false);
			for (String dataFieldValue : dataFieldValues) {
				String translatedValue = translateProperties.get(dataFieldValue);
				translatedValue = (translatedValue != null) ? translatedValue : dataFieldValue;
				translatedValues.add(translatedValue);
			}
		}
		return translatedValues;
	}

	
	private static String getTranslateFileName(String dataRule) {
		String translateFileName = null;
		if (dataRule.startsWith("translateValue") || dataRule.startsWith("translateConnectedSubfields")) {
			Pattern patternPropFile = java.util.regex.Pattern.compile("\\[.*?\\]");
			Matcher matcherPropFile = patternPropFile.matcher(dataRule);
			if(matcherPropFile.find()) {
				translateFileName = matcherPropFile.group();
				translateFileName = translateFileName.replace("[", "").replace("]", "");
			}
		}		
		return translateFileName;
	}

}