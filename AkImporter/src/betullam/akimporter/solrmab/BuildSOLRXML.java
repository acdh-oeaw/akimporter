package betullam.akimporter.solrmab;

import java.io.File;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class BuildSOLRXML {

	DocumentBuilderFactory docFactory;
	DocumentBuilder docBuilder;
	List<Record> listOfRecords;
	
	public void buildXML(RecordSet recordSet) {
		
		try {
			
			docFactory = DocumentBuilderFactory.newInstance();
			docBuilder = docFactory.newDocumentBuilder();
			listOfRecords  = recordSet.getRecords();

			// Create add-element
			Document xmlDocument = docBuilder.newDocument();
			Element add = xmlDocument.createElement("add");
			xmlDocument.appendChild(add);
			

			for (Record record : listOfRecords) {
				
				// For each Record, creat a doc-element:
				Element doc = xmlDocument.createElement("doc");
				add.appendChild(doc);
				
				
				for (Controlfield cf : record.getControlfields()) {
					// For each field (datafield, controlfield, etc.), create a field-element:
					Element field = xmlDocument.createElement("field");
					
					// Set attribute(s) of the field-element created before:
					field.setAttribute("name", cf.getTag());
					
					// Set content of the field-element created before:
					field.setTextContent(cf.getControlfieldText());
					
					// Append the field-node to the doc-node:
					doc.appendChild(field);
				}
				
				for (Mabfield mf : record.getMabfields()) {					
					Element field = xmlDocument.createElement("field");
					doc.appendChild(field);
					field.setAttribute("name", mf.getFieldname());
					field.setTextContent(mf.getFieldvalue());
				}

				/*
				for (Datafield df : record.getDatafields()) {
					Element field = xmlDocument.createElement("field");
					doc.appendChild(field);
					field.setAttribute("name", df.getTag() + df.getInd1() + df.getInd2());		
					
					for (Subfield sf : df.getSubfields()) {
						Element subfield = xmlDocument.createElement("field");
						doc.appendChild(subfield);
						subfield.setAttribute("name", sf.getCode());
						subfield.setTextContent(sf.getSubfieldText());
					}
				}
				*/

			}
			
			
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(xmlDocument);
			StreamResult result = new StreamResult(new File("C:\\mab_conversion\\file.xml"));
	 
			// Output to console for testing
			//StreamResult result = new StreamResult(System.out);
	 
			transformer.transform(source, result);
	 
			System.out.println("XML File created!");
			
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		
	}

}
