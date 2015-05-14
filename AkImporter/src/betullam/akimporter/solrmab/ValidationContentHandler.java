package betullam.akimporter.solrmab;

import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

public class ValidationContentHandler implements ContentHandler {

	private String nodeContent;
	private boolean isSYS;
	

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attribs) throws SAXException {
		nodeContent = "";
		
		isSYS = false;
		
		if(localName.equals("controlfield")) {
			String tag = attribs.getValue("tag");
			isSYS = (tag.equals("SYS")) ? true : false;
		}
	}
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		// Reads the content of the current XML-node:
		nodeContent += new String(ch, start, length);	
	}
	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if (isSYS == true) {
			System.out.println("Validating SYS-No: " + nodeContent);
		}
	}
	
	
	
	
	@Override
	public void startDocument() throws SAXException { }

	@Override
	public void endDocument() throws SAXException { }

	@Override
	public void setDocumentLocator(Locator locator) { }
	
	@Override
	public void startPrefixMapping(String prefix, String uri) throws SAXException { }

	@Override
	public void endPrefixMapping(String prefix) throws SAXException { }

	@Override
	public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException { }

	@Override
	public void processingInstruction(String target, String data) throws SAXException { }

	@Override
	public void skippedEntity(String name) throws SAXException { }

}
