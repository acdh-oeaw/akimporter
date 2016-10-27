/**
 * Parses the contents of Mets/Mods files.
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

import org.apache.solr.client.solrj.SolrServer;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class MetsContentHandler implements ContentHandler {

	private List<PropertiesObject> propertiesObjects;
	private SolrServer solrServer;
	private String elementContent;
	private String timeStamp;
	private boolean print = true;
	
	public MetsContentHandler(List<PropertiesObject> propertiesObjects, SolrServer solrServer, String timeStamp, boolean print) {
		this.propertiesObjects = propertiesObjects;
		this.solrServer = solrServer;
		this.timeStamp = timeStamp;
		this.print = print;
	}
	
	@Override
	public void startDocument() throws SAXException {
		
	}
	
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
		
	}

	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		
	}

	
	@Override
	public void endDocument() throws SAXException {	
	}
	
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {	
		elementContent += new String(ch, start, length);
	}

	
	/**
	 * Prints the specified text to the console if "print" is true.
	 * 
	 * @param print		boolean: true if the text should be print, false otherwise
	 * @param text		String: a text message to print.
	 */
	private void print(boolean print, String text) {
		if (print) {
			System.out.print(text);
		}
	}
	
	// Methods of ContentHandler that are not used at the moment
	@Override
	public void setDocumentLocator(Locator locator) {}
	
	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException {}

	@Override
	public void endPrefixMapping(String prefix) throws SAXException {}

	@Override
	public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {}

	@Override
	public void processingInstruction(String target, String data) throws SAXException {}

	@Override
	public void skippedEntity(String name) throws SAXException {		}

}
