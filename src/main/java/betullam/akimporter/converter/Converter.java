package main.java.betullam.akimporter.converter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.MarcWriter;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class Converter {

	
	private List<String> convertedFiles = new ArrayList<String>();
	
	/**
	 * Converts a Marc21 binary file that contains one or multiple Marc records to MarcXML records. Each record will be written into 
	 * one single XML file that is saved to the folder specified by "folderNameMarcXmlOutput".
	 * 
	 * @param fileNameMarcBinInput		Marc21 binary file with one or multiple Marc records.
	 * @param folderNameMarcXmlOutput	Path to a folder where the resulting XML files (one per record in the binary file) are saved.
	 */
	public void marcBin2MarcXml(String fileNameMarcBinInput, String folderNameMarcXmlOutput) {		
		InputStream inputBin = null;
		MarcReader marcBinReader;
		OutputStream outputXml = null;
		MarcWriter marcXmlWriter = null;
		
		try {
			File inputFile = new File(fileNameMarcBinInput);
			String inputFileName = inputFile.getName();
			inputBin = new FileInputStream(fileNameMarcBinInput);
			marcBinReader = new MarcStreamReader(inputBin, "UTF8");
			
			int counter = 0;
			while (marcBinReader.hasNext()) {
				Record marcBinRecord = marcBinReader.next();
				String outputFileName = AkImporterHelper.stripFileSeperatorFromPath(folderNameMarcXmlOutput) + File.separator + inputFileName + "_" + counter + ".xml";
				outputXml = new FileOutputStream(outputFileName);
				marcXmlWriter = new MarcXmlWriter(outputXml, "UTF8", true); // Use "System.out" to output to console for testing
				marcXmlWriter.write(marcBinRecord);
				
				this.convertedFiles.add(outputFileName);
				
				marcXmlWriter.close();
				outputXml.close();
				counter++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				inputBin.close();
				outputXml.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public List<String> getConvertedFiles() {
		return this.convertedFiles;
	}
	
}
