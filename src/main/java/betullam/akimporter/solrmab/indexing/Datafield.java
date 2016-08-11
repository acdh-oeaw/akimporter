/**
 * Datafield class
 *
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
 * 
 * TODO: Check if this class is still necessary!
 * 		 Maybe we could yous Mabfield class instead.
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

import java.util.ArrayList;

public class Datafield {

	private String tag;
	private String ind1;
	private String ind2;
	//private List<Mabfield> mabfields;
	private ArrayList<Subfield> subfields;

	public Datafield() {}


	public String getTag() {
		return this.tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
	
	public String getInd1() {
		return this.ind1;
	}

	public void setInd1(String ind1) {
		this.ind1 = ind1;
	}
	
	public String getInd2() {
		return this.ind2;
	}

	public void setInd2(String ind2) {
		this.ind2 = ind2;
	}
	
	public ArrayList<Subfield> getSubfields() {
		return subfields;
	}

	public void setSubfields(ArrayList<Subfield> subfields) {
		this.subfields = subfields;
	}
	
	/*
	public List<Mabfield> getSubfields() {
		return this.mabfields;
	}

	public void setSubfields(List<Mabfield> mabfields) {
		this.mabfields = mabfields;
	}
	*/

	@Override
	public String toString() {
		return "Datafield [tag=" + tag + ", ind1=" + ind1 + ", ind2=" + ind2 + ", subfields=" + subfields + "]";
	}
}