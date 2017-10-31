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
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import main.java.betullam.akimporter.main.AkImporterHelper;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;

public class FtpDownload {

	/**
	 * Downloads a files from an FTP-Server.
	 * @param remotePath		Path to a directory in which the file for downloading are stored.
	 * @param remotePathMoveTo	Path to which the downloaded files should be moved after downloading.
	 * @param localPathTarGz	Local path where the downloaded files should be stored.
	 * @param host				FTP host name.
	 * @param port				FTP port.
	 * @param user				FTP username.
	 * @param password			FTP password.
	 * @param showMessages		True if status messages should be printed to console.
	 * @return					True if download process was successful.
	 */
	public boolean downloadFiles(String remotePath, String remotePathMoveTo, String localPathTarGz, String host, int port, String user, String password, String timeStamp, boolean showMessages) {
		boolean ftpOk = false;
		FTPClient ftpClient = new FTPClient();
		AkImporterHelper.print(showMessages, "\nDownloading data from " + host + " to "+localPathTarGz+" ... ");
		
		try {

			ftpClient.connect(host, port);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpOk &= ftpClient.login(user, password);

			FTPFile[] ftpFiles = ftpClient.listFiles(remotePath);
			int fileCounter = 0;
			
			for (FTPFile ftpFile : ftpFiles) {
				boolean success = false;
				if (ftpFile.isFile()) {
					fileCounter++;
					String fileName = ftpFile.getName();
					File localFile = new File(localPathTarGz + File.separator + fileName);
					OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFile));
					ftpClient.setFileType(FTP.BINARY_FILE_TYPE); // Set this here, not further above! IMPORTANT!
					success = ftpClient.retrieveFile(remotePath + File.separator + fileName, outputStream);

					if (success) {
						ftpOk = true;
					} else {
						ftpOk = false;
						AkImporterHelper.print(showMessages, "ERROR downloading file \"" + fileName + "\" from FTP-Server!\n");
					}
					outputStream.close();
				}
			}

			if (fileCounter > 0 && remotePathMoveTo != null && !remotePathMoveTo.equals("")) {
				// Check if "move to" directory exists. If not, create it (including subdirectories).
				boolean moveToDirectoryExists = false;
				remotePathMoveTo = AkImporterHelper.stripFileSeperatorFromPath(remotePathMoveTo) + File.separator + timeStamp;
				String[] subDirectories = remotePathMoveTo.split("/");

				for (String subDirectory : subDirectories) {
					boolean hasSubDirectory = ftpClient.changeWorkingDirectory(subDirectory);
					if (!hasSubDirectory) {
						boolean madeDirectory = ftpClient.makeDirectory(subDirectory);
						if (madeDirectory) {
							ftpClient.changeWorkingDirectory(subDirectory);
							moveToDirectoryExists = true;
							//System.out.println("Created subdirectory " + subDirectory);
						} else {
							System.err.println("Error creating directory " + subDirectory + " on FTP server " + host + ".");
							moveToDirectoryExists = false;
							break;
						}
					} else {
						moveToDirectoryExists = true;
					}
				}

				if (moveToDirectoryExists) {
					ftpClient.changeWorkingDirectory("/");
					for (FTPFile ftpFile : ftpFiles) {
						if (ftpFile.isFile()) {
							String from = AkImporterHelper.stripFileSeperatorFromPath(remotePath) + File.separator + ftpFile.getName();
							String to = AkImporterHelper.stripFileSeperatorFromPath(remotePathMoveTo) + File.separator + ftpFile.getName();

							boolean moveSuccess = ftpClient.rename(from, to);
							if (!moveSuccess) {
								System.err.println("Error moving from " + from + " to " + to);
							}
						}
					}
				}				
			} else if (fileCounter == 0) {
				ftpOk = true; // Everything is OK ... there was just nothing to download.
			}

			AkImporterHelper.print(showMessages, "Done\n");
			ftpClient.logout();
			ftpClient.disconnect();

		} catch (IOException e) {
			ftpOk = false;
			e.printStackTrace();
		}
		return ftpOk;
	}
	
	
	public boolean downloadFilesSftp(String remotePath, String remotePathMoveTo, String localPathTarGz, String host, int port, String user, String password, String hostKey, String timeStamp, boolean showMessages) {
		boolean sftpOk = false;
		SSHClient ssh = new SSHClient();
		ssh.addHostKeyVerifier(hostKey);
		
		try {
			ssh.loadKnownHosts();
			ssh.connect(host, port);
		    ssh.authPassword(user, password);
		    SFTPClient sftp = ssh.newSFTPClient();
		    try {
		    	List<RemoteResourceInfo> fileInfos = sftp.ls(remotePath);
		    	for (RemoteResourceInfo fileInfo : fileInfos) {
		    		if (fileInfo.isRegularFile()) {
		    			System.out.println("File: " + fileInfo.getPath());
		    			//sftp.get(fileInfo.getPath(), localPathTarGz);
		    		}
		    	}
		        
		    } finally {
		        sftp.close();
		        sftpOk = true;
		    }
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
		    try {
				ssh.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sftpOk;
	}

}
