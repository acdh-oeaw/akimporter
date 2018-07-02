/**
 * Extracting tar.gz-file.
 * Used for updating data changes that are delivered as tar.gz-file.
 *  
 * Copyright (C) AK Bibliothek Wien 2015, Michael Birkner
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
package main.java.betullam.akimporter.updater;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import main.java.betullam.akimporter.main.AkImporterHelper;

public class ExtractTarGz {

	
	List<String> extractedFiles = new ArrayList<String>();
	
	/**
	 * Extracting compressed file (generic)
	 * 
	 * @param pathToCompressed	Full path to a directory (containing at least one compressed file) or single compressed file that should be extracted.
	 * @param timeStamp			Timestamp when the update process was started (used for renaming files).
	 * @param pathToExtracted	Path to a directory where the extracted content should be stored. Will be created if it does not exist.
	 */
	public void extractGeneric(String pathToCompressed, String timeStamp, String pathToExtracted) {
		File pathOriginal = new File(pathToCompressed);
		String fileToExtract = null;
		File decompressedFile = null;
		
		// Check if we have a single compressed file or a whole directory with compressed files
		File[] filesOriginal = null;
		if (pathOriginal.isFile()) {
			List<File> filesOriginalList = new ArrayList<File>();
			filesOriginalList.add(pathOriginal);
			filesOriginal = filesOriginalList.toArray(new File[0]);
		} else {
			filesOriginal = pathOriginal.listFiles();
		}
		

		try {
			for (File fileOriginal : filesOriginal) {
				if (fileOriginal.isFile()) {

					// Decompress compressed file if applicable
					FileInputStream compressedFis = null;
					BufferedInputStream compressedBin = null;
					OutputStream decompressedOs = null;
					CompressorInputStream compressorIs = null;
					try {
						compressedFis = new FileInputStream(fileOriginal);
						compressedBin = new BufferedInputStream(compressedFis);
						compressorIs = new CompressorStreamFactory().createCompressorInputStream(compressedBin);
						
						fileToExtract = fileOriginal.getAbsolutePath() + "." + timeStamp + ".tar";
						decompressedFile = new File(fileToExtract);
						decompressedOs = Files.newOutputStream(Paths.get(fileToExtract));

						final byte[] compressorBuffer = new byte[1024];
						int i = 0;
						while (-1 != (i = compressorIs.read(compressorBuffer))) {
							decompressedOs.write(compressorBuffer, 0, i);
						}
					} catch (CompressorException e) {
						// No compression! Skip to archiver.
						fileToExtract = fileOriginal.getAbsolutePath();
					} finally {
						// Close streams
						if (compressedFis != null) {
							compressedFis.close();
						}
						if (compressedBin != null) {
							compressedBin.close();
						}
						if (decompressedOs != null) {
							decompressedOs.close();
						}
						if (compressorIs != null) {
							compressorIs.close();
						}
					}
					
					// De-archive the archive file
					FileInputStream archivedFis = null;
					BufferedInputStream archivedBin = null;
					FileOutputStream extractedFos = null;
					ArchiveInputStream archiverIs = null;
					try {
						ArchiveEntry archiveEntry = null;
						archivedFis = new FileInputStream(fileToExtract);
						archivedBin = new BufferedInputStream(archivedFis);				
						archiverIs = new ArchiveStreamFactory().createArchiveInputStream(archivedBin);
						
						// Create destination directory
						AkImporterHelper.mkDirIfNotExists(pathToExtracted);
						
						// Process archive entry
						final byte[] archiveBuffer = new byte[1024];
						archiveEntry = archiverIs.getNextEntry();
						while (archiveEntry != null) {
							File destPath = new File(pathToExtracted + File.separator + archiveEntry.getName());
							if (!archiveEntry.isDirectory()) {
								extractedFos = new FileOutputStream(destPath); // Write archived file to output, e. g. to an XML file
								int j = 0;
								while (-1 != (j = archiverIs.read(archiveBuffer))) {
									extractedFos.write(archiveBuffer, 0, j);
								}
								extractedFos.close();
								this.extractedFiles.add(destPath.getAbsolutePath());
							}
							else {
								destPath.mkdir();
							}
							archiveEntry = archiverIs.getNextEntry();
						}
					} catch (ArchiveException e) {
						// No known archive file! Skip it.
						decompressedFile = null;
					} finally {
						// Close streams
						if (archivedFis != null) {
							archivedFis.close();
						}
						if (archivedBin != null) {
							archivedBin.close();
						}
						if (extractedFos != null) {
							extractedFos.flush();
							extractedFos.close();
						}
						if (archiverIs != null) {
							archiverIs.close();
						}
					}
					
					// Delete decompressed file if it exists
					if (decompressedFile != null && decompressedFile.exists()) {
						decompressedFile.delete();
					}
				}
			}
		} catch (FileNotFoundException e) {
			System.err.println("Error while extracting files!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error while extracting files!");
			e.printStackTrace();
		}		
	}
	
	/**
	 * Extracting a tar.gz file
	 * 
	 * @param pathToTarGzs		Full path to a directory containing at least one tar.gz file that should be extracted.
	 * @param timeStamp			Timestamp when the update process was started (used for renaming files).
	 * @param pathToExtracted	Path to a directory where the extracted content should be stored.
	 */
	public void extractTarGz(String pathToTarGzs, String timeStamp, String pathToExtracted) {
		FileInputStream gzFis = null;
		BufferedInputStream gzBin = null;
		BufferedInputStream tarBin = null;
		FileOutputStream tarFos = null;
		GzipCompressorInputStream gzCis = null;
		
		FileInputStream tarFis = null;
		TarArchiveInputStream tarAis = null;
		FileOutputStream xmlFos = null;
		TarArchiveEntry tarEntry = null;
		
		File pathTarGz = new File(pathToTarGzs);
		final byte[] buffer = new byte[1024];
		try {
			for (File fileTarGz : pathTarGz.listFiles()) {
				if (fileTarGz.isFile()) {

					// Unzip GZ:
					String tarFilename = fileTarGz.getName().replace(".tar.gz", "") + "." + timeStamp + ".tar";
					String pathToTar = pathTarGz + File.separator + tarFilename;
					gzFis = new FileInputStream(fileTarGz);
					gzBin = new BufferedInputStream(gzFis);
					gzCis = new GzipCompressorInputStream(gzBin);
					tarFos = new FileOutputStream(pathToTar);
					int i = 0;
					while (-1 != (i = gzCis.read(buffer))) {
						tarFos.write(buffer, 0, i);
					}
					gzFis.close();
					gzBin.close();
					tarFos.close();
					gzCis.close();

					// Extract TAR:
					File tarFile = new File(pathToTar);
					tarFis = new FileInputStream(tarFile);
					tarBin = new BufferedInputStream(tarFis);
					tarAis = new TarArchiveInputStream(tarBin);
					tarEntry = tarAis.getNextTarEntry();
					while (tarEntry != null) {
						File destPath = new File(pathToExtracted + File.separator + tarEntry.getName());
						if (!tarEntry.isDirectory()) {
							xmlFos = new FileOutputStream(destPath);
							int j = 0;
							while (-1 != (j = tarAis.read(buffer))) {
								xmlFos.write(buffer, 0, j);
							}
							xmlFos.close();
						}
						else {
							destPath.mkdir();
						}
						tarEntry = tarAis.getNextTarEntry();
					}
					tarFis.close();
					tarBin.close();
					tarAis.close();
					
					// Remove TAR:
					tarFile.delete();
				}
			}

		} catch (FileNotFoundException e) {
			System.err.println("Error while extracting files!");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Error while extracting files!");
			e.printStackTrace();
		}
	}

	
	public List<String> getExtractedFiles() {
		return this.extractedFiles;
	}


}
