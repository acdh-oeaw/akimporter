package main.java.betullam.akimporter.converter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcWriter;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;

public class Converter {

	public void marcBin2MarcXml(String fileNameMarcBin) {		
		InputStream input;
		try {
			input = new FileInputStream(fileNameMarcBin);
			MarcReader marcBinReader = new MarcStreamReader(input);
			MarcWriter marcXmlWriter = new MarcXmlWriter(System.out, true);
			while (marcBinReader.hasNext()) {
				Record marcBinRecord = marcBinReader.next();
				marcXmlWriter.write(marcBinRecord);
			}
			marcXmlWriter.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
