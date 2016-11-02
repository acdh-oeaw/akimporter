/**
 * MetsRawRecord class which describes a Mets/Mods record
 * 
 * Copyright (C) AK Bibliothek Wien 2016, Michael Birkner
 * 
 * This file is part of AkImporter.
 * 
 * AkImporter is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * AkImporter is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with AkImporter.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author   Michael Birkner <michael.birkner@akwien.at>
 * @license  http://www.gnu.org/licenses/gpl-3.0.html
 * @link     http://wien.arbeiterkammer.at/service/bibliothek/
 */
package main.java.betullam.akimporter.solrmab.indexing;

import java.util.List;

public class MetsRawRecord {

	String topParentTitle;
	String topParentType;
	String topParentContentId;
	String topParentLogId;
	String topTitle;
	String topSubtitle;
	String topTitleSort;
	List<Person> topPersons;
	String topYear;
	String topVolume;
	String topIssueNo;
	String topSortNo;
	String topPlace;
	String topPublisher;
	String topLanguage;
	List<String> topClassification;
	String topType;
	String topAkIdentifier;
	String topAcNo;
	String topGoobiId;
	String topContentId;
	String topDmdLogId;
	String topLogId;
	String topPhysId;
	List<Child> childs;

	private class Child {
		String childTitle;
		String childSubtitle;
		String childTitleSort;
		List<Person> childPersons;
		List<String> childAbstracts;
		String childLanguage;
		List<String> childClassification;
		String childFromPage;
		String childToPage;
		String childType;
		String childContentId;
		String childDmdLogId;
		String childLogId;
		String childPhysId;
	}
	
	private class Person {
		String personFirstName;
		String personLastName;
		String personRole;
		String personAuthorityId;
	}
}
