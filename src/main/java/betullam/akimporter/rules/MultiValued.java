package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.List;

public class MultiValued {
	
	public static List<String> getFirstValue(List<String> treatedValues) {
		List<String> firstValueInList = new ArrayList<String>();
		String firstValue = treatedValues.get(0);
		firstValueInList.add(firstValue);
		return firstValueInList;
	}
	
	public static String getSingleValue(List<String> treatedValues) {
		// Return only the first value of our treated values:
		return treatedValues.get(0);
	}
	
}