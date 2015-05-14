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
