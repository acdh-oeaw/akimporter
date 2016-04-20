/**
 * Downloading file from an FTP-Server.
 * Used for updating data changes that are delivered as tar.gz-file to an FTP-Server.
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class FtpDownload {

	/**
	 * Downloads a files from an FTP-Server.
	 * @param remotePath		Path to a directory in which the file for downloading are stored.
	 * @param localPathTarGz	Local path where the downloaded files should be stored.
	 * @param host				FTP host name.
	 * @param port				FTP port.
	 * @param user				FTP username.
	 * @param password			FTP password.
	 * @param showMessages		True if status messages should be printed to console.
	 * @return					True if download process was successful.
	 */
	public boolean downloadFiles(String remotePath, String localPathTarGz, String host, int port, String user, String password, boolean showMessages) {
		boolean ftpOk = false;
		FTPClient ftpClient = new FTPClient();

		if( showMessages ) { System.out.print( "\nDownloading data from " + host + " to "+localPathTarGz+" ... "); }
		
		try {

			ftpClient.connect( host, port );
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpOk &= ftpClient.login( user, password );

			FTPFile[] ftpFiles = ftpClient.listFiles(remotePath);

			for (FTPFile ftpFile : ftpFiles) {
				boolean success = false;
				if (ftpFile.isFile()) {
					String fileName = ftpFile.getName();
					File localFile = new File(localPathTarGz + File.separator + fileName);
					OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFile));
					ftpClient.setFileType(FTP.BINARY_FILE_TYPE); // Set this here, not further above! IMPORTANT!
					success = ftpClient.retrieveFile(remotePath + File.separator + fileName, outputStream);

					if (success) {
						ftpOk = true;
						if( showMessages ) { System.out.print("Done\n"); }
					} else {
						ftpOk = false;
						if( showMessages ) { System.out.println("ERROR downloading file \"" + fileName + "\" from FTP-Server!"); }
					}
					outputStream.close();
				}
			}
			
			ftpClient.logout();
			ftpClient.disconnect();
		} catch (IOException e) {
			ftpOk = false;
			e.printStackTrace();
		}
		return ftpOk;
	}

}
