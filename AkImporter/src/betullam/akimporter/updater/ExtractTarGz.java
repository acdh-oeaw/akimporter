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
package betullam.akimporter.updater;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class ExtractTarGz {

	/**
	 * Extracting a tar.gz file
	 * 
	 * @param pathToTarGz		Full path to a tar.gz file that should be extracted.
	 * @param timeStamp			Timestamp when the update process was started (used for renaming files).
	 * @param pathToExtracted	Path to a directory where the extracted content should be stored.
	 */
	public void extractTarGz(String pathToTarGz, String timeStamp, String pathToExtracted) {
		FileInputStream gzFis = null;
		BufferedInputStream gzBin = null;
		BufferedInputStream tarBin = null;
		FileOutputStream tarFos = null;
		GzipCompressorInputStream gzCis = null;
		
		FileInputStream tarFis = null;
		TarArchiveInputStream tarAis = null;
		FileOutputStream xmlFos = null;
		TarArchiveEntry tarEntry = null;
		
		File pathTarGz = new File(pathToTarGz);
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}




	}

}
