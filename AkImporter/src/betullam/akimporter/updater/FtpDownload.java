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
package betullam.akimporter.updater;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class FtpDownload {


	public boolean downloadFiles(String remotePath, String localPathTarGz, String host, int port, String user, String password, boolean showMessages) {
		boolean ftpOk = false;
		FTPClient ftpClient = new FTPClient();

		if( showMessages ) { System.out.println( "Start FTP Download from " + host + " ..."); }
		
		try {

			ftpClient.connect( host, port );
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			if( showMessages ) { System.out.print( "FTP Reply: " + ftpClient.getReplyString() ); }
			ftpOk &= ftpClient.login( user, password );
			if( showMessages ) { System.out.print( "FTP Reply: " + ftpClient.getReplyString() ); }

			FTPFile[] ftpFiles = ftpClient.listFiles(remotePath);

			for (FTPFile ftpFile : ftpFiles) {
				boolean success = false;
				if (ftpFile.isFile()) {
					String fileName = ftpFile.getName();
					File localFile = new File(localPathTarGz + File.separator + fileName);
					OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFile));
					ftpClient.setFileType(FTP.BINARY_FILE_TYPE); // Set this here, not further above! IMPORTANT!
					success = ftpClient.retrieveFile(remotePath + File.separator + fileName, outputStream);
					//ftpClient.getReplyString();
					if( showMessages ) { System.out.print( "FTP Reply: " + ftpClient.getReplyString() ); }

					if (success) {
						ftpOk = true;
						if( showMessages ) { System.out.println("Successfully downloaded file \"" + fileName + "\""); }
					} else {
						ftpOk = false;
						if( showMessages ) { System.out.println("ERROR downloading file from FTP-Server!"); }
					}
					outputStream.close();
				}
			}

			ftpClient.logout();
			if( showMessages ) { System.out.println( "Logging out of FTP Server " + host + "." ); }
			if( showMessages ) { System.out.print( "FTP Reply: " + ftpClient.getReplyString() ); }
			ftpClient.disconnect();
			if( showMessages ) { System.out.println( "FTP Client disconnected" ); }
		} catch (IOException e) {
			ftpOk = false;
			e.printStackTrace();
		}
		return ftpOk;
	}

}
