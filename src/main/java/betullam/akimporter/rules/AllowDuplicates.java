package main.java.betullam.akimporter.rules;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class AllowDuplicates {
	
	public static List<String> getDeduplicatedList(List<String> treatedValues) {
		List<String> dedupList = new ArrayList<String>();
		LinkedHashSet<String> dedupSet = new LinkedHashSet<String>();
		for (String treatedValue : treatedValues) {
			dedupSet.add(treatedValue);
		}
		dedupList.addAll(dedupSet);
		return dedupList;
	}
	
}