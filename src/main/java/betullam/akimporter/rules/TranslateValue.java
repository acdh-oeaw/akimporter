package main.java.betullam.akimporter.rules;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class TranslateValue {

	public static String getTranslatedValue(String value, String dataRule) {
		String translatedValue = null;
		String translateFileName = getTranslateFileName(dataRule);
		if (translateFileName != null) {
			Map<String, String> translateProperties = AkImporterHelper.getTranslateProperties(translateFileName, Rules.getOaiPropertiesFilePath(), false);
			translatedValue = translateProperties.get(value);
			translatedValue = (translatedValue != null) ? translatedValue : value;
		}

		return translatedValue;
	}

	private static String getTranslateFileName(String dataRule) {
		String translateFileName = null;
		if (dataRule.startsWith("translateValue")) {
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
