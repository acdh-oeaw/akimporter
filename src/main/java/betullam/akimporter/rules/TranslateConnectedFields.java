package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.List;

public class TranslateConnectedFields {

	public static List<String> translate(List<String> dataFieldValues, String dataRule) {
		List<String> returnValue = new ArrayList<String>();		
		returnValue = TranslateValue.getTranslatedValues(dataFieldValues, dataRule);
		return returnValue;
	}

}