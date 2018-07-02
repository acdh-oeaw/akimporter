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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import main.java.betullam.akimporter.main.AkImporterHelper;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;

public class FtpDownload {
	
	List<String> downloadedFiles = new ArrayList<String>();

	/**
	 * Downloads a files from an FTP-Server.
	 * @param remotePath		Path to a directory in which the file for downloading are stored.
	 * @param remotePathMoveTo	Path to which the downloaded files should be moved after downloading.
	 * @param localPath	Local path where the downloaded files should be stored.
	 * @param host				FTP host name.
	 * @param port				FTP port.
	 * @param user				FTP username.
	 * @param password			FTP password.
	 * @param compareFiles		Compare files between local and remote path to avoid multiple download and indexing of data.
	 * @param showMessages		True if status messages should be printed to console.
	 * @return					True if download process was successful.
	 */
	public boolean downloadFiles(String remotePath, String remotePathMoveTo, String localPath, String host, int port, String user, String password, String timeStamp, boolean compareFiles, boolean showMessages) {
		boolean ftpOk = false;
		FTPClient ftpClient = new FTPClient();
		
		try {
			ftpClient.connect(host, port);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			ftpOk &= ftpClient.login(user, password);
			
			List<String> filesToDownload = null;
			if (compareFiles) {
				AkImporterHelper.print(showMessages, "\nStart comparing file trees and getting the difference ... ");
				filesToDownload = this.getFileTreeDiff(ftpClient, localPath, remotePath, host, port, user, password, showMessages);
				AkImporterHelper.print(showMessages, "Done");			
			} else {
				filesToDownload = new ArrayList<String>();
				FTPFile[] ftpFiles = ftpClient.listFiles(remotePath);
				for (FTPFile ftpFile : ftpFiles) {
					if (ftpFile.isFile()) {
						filesToDownload.add(ftpFile.getName());
					}
				}
				
				// Get remote file names recursively
				filesToDownload = getRemoteFiles(ftpClient, remotePath, "", new ArrayList<String>());				
			}
			
			int fileCounter = 0;
			if (filesToDownload != null && !filesToDownload.isEmpty()) {
				AkImporterHelper.print(showMessages, "\nDownloading data from FTP " + host + " to "+localPath+" ... ");
				for (String fileToDownload : filesToDownload) {
					boolean success = false;
					fileCounter++;
					
					// Get filename and (sub)directories from path.
					Path path = Paths.get(fileToDownload);
					String directories = (path.getParent() != null) ? File.separator + path.getParent().toString() : "";
					AkImporterHelper.mkDirIfNotExists(localPath + directories); // Create directories if they don't exist
					String fileName = path.getFileName().toString();
					String localFileFullPath = localPath + directories + File.separator + fileName;
					
					// Set local file
					File localFile = new File(localFileFullPath);
					
					OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(localFile));
					ftpClient.setFileType(FTP.BINARY_FILE_TYPE); // Set this here, not further above! IMPORTANT!
					
					success = ftpClient.retrieveFile(remotePath + File.separator + fileToDownload, outputStream);
					if (success) {
						this.downloadedFiles.add(localFileFullPath);
						ftpOk = true;
					} else {
						ftpOk = false;
						System.err.println("ERROR downloading file \"" + fileName + "\" from FTP-Server!");
					}
					outputStream.close();
				}
				AkImporterHelper.print(showMessages, "Done");
			} else {
				AkImporterHelper.print(showMessages, "\nThere are no files to download!");
			}
			
			// Move files on remote path if applicable
			if (fileCounter > 0 && remotePathMoveTo != null && !remotePathMoveTo.trim().equals("")) {
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

					for (String fileToDownload : filesToDownload) {
						// Get filename from path
						Path path = Paths.get(fileToDownload);
						String directories = (path.getParent() != null) ? File.separator + path.getParent().toString() : "";
						String fileName = path.getFileName().toString();
						
						// Move files including subdirectories
						String from = AkImporterHelper.stripFileSeperatorFromPath(remotePath) + directories + File.separator + fileName;
						String to = AkImporterHelper.stripFileSeperatorFromPath(remotePathMoveTo) + directories + File.separator + fileName;

						boolean moveSuccess = ftpClient.rename(from, to);
						if (!moveSuccess) {
							System.err.println("Error moving from " + from + " to " + to);
						}
					}
				}				
			} else if (fileCounter == 0) {
				ftpOk = true; // Everything is OK ... there was just nothing to download.
			}
			
			ftpClient.logout();
			ftpClient.disconnect();

		} catch (IOException e) {
			ftpOk = false;
			System.err.println("Error while downloading file using FTP");
			e.printStackTrace();
		}
		return ftpOk;
	}
	
	
	public boolean downloadFilesSftp(String remotePath, String remotePathMoveTo, String localPathTarGz, String host, int port, String user, String password, String hostKey, String timeStamp, boolean showMessages) {

		boolean sftpOk = false;
		SSHClient ssh = new SSHClient();
		ssh.addHostKeyVerifier(hostKey);
		AkImporterHelper.print(showMessages, "\nDownloading data from SFTP " + host + " to "+localPathTarGz+" ... ");

		try {
			ssh.loadKnownHosts();
			ssh.connect(host, port);
			ssh.authPassword(user, password);
			SFTPClient sftpClient = ssh.newSFTPClient();
			//StatefulSFTPClient sftpClient = new StatefulSFTPClient(new SFTPEngine(ssh));

			try {
				List<RemoteResourceInfo> fileInfos = sftpClient.ls(remotePath);
				int fileCounter = 0;

				for (RemoteResourceInfo fileInfo : fileInfos) {
					if (fileInfo.isRegularFile()) {
						fileCounter++;
						String fileName = fileInfo.getName();

						// Download files
						sftpClient.get(remotePath + File.separator + fileName, localPathTarGz + File.separator + fileName);
					}
				}

				if (fileCounter > 0 && remotePathMoveTo != null && !remotePathMoveTo.trim().equals("")) {
					// Check if "move to" directory exists. If not, create it (including subdirectories).
					remotePathMoveTo = AkImporterHelper.stripFileSeperatorFromPath(remotePathMoveTo) + File.separator + timeStamp;

					// Make "move to" directories
					sftpClient.mkdirs(remotePathMoveTo);

					// Move files
					if (sftpClient.statExistence(remotePathMoveTo) != null) {
						for (RemoteResourceInfo fileInfo : fileInfos) {
							if (fileInfo.isRegularFile()) {
								String from = AkImporterHelper.stripFileSeperatorFromPath(remotePath) + File.separator + fileInfo.getName();
								String to = AkImporterHelper.stripFileSeperatorFromPath(remotePathMoveTo) + File.separator + fileInfo.getName();
								sftpClient.rename(from, to);
							}
						}
					}				
				}

				sftpOk = true;
			} catch (Exception e) {
				sftpOk = false;
				System.err.println("Error while downloading file using SFTP");
				e.printStackTrace();
			} finally {
				sftpClient.close();
			}

		} catch (IOException e) {
			System.err.println("Error while downloading file using SFTP");
			e.printStackTrace();
		} finally {
			try {
				ssh.disconnect();
			} catch (IOException e) {
				System.err.println("Error while downloading file using SFTP");
				e.printStackTrace();
			}
		}
		AkImporterHelper.print(showMessages, "Done");
		return sftpOk;
	}
	
	
	private List<String> getFileTreeDiff(FTPClient ftpClient, String localBasePath, String remoteBasePath, String host, int port, String user, String pass, boolean print) {		
		List<String> filesToDownload = null;
		
		// Get local file names
		List<String> localFileNames = null;
		localFileNames = getLocalFiles(localBasePath);
		
		// Get remote file names recursively
		List<String> remoteFileNames = null;
		remoteFileNames = getRemoteFiles(ftpClient, remoteBasePath, "", new ArrayList<String>());
		
		// Compare the two lists (local and remote)
		remoteFileNames.removeAll(localFileNames);
		
		// Check if there is a difference between the lists
		if (!remoteFileNames.isEmpty()) {
			filesToDownload = remoteFileNames;
		}
		
		return filesToDownload;
	}
	
	
	private List<String> getLocalFiles(String localBasePath) {
		List<String> localFileNames = new ArrayList<String>();
		
		File localBaseFile = new File(localBasePath);
		List<File> localFiles = new ArrayList<File>();
		if (localBaseFile.isDirectory()) {
			localFiles = (List<File>)FileUtils.listFiles(localBaseFile, null, true);
		}
		
		for(File localFile : localFiles) {
			String localRelativeFilePath = new File(localBasePath).toURI().relativize(localFile.toURI()).getPath();
			localFileNames.add(localRelativeFilePath);
		}
		
		return localFileNames;
	}
	
	
	private List<String> getRemoteFiles(FTPClient ftpClient, String remoteBasePath, String remoteRelativePath, List<String> remoteFileNames) {
		FTPFile[] ftpFiles;
		try {
			
			// Get files in current FTP folder
			ftpFiles = ftpClient.listFiles(remoteBasePath + File.separator + remoteRelativePath);
			
			// Iterate over files
			for (FTPFile ftpFile : ftpFiles) {
				// Variable for file name
				String relativeFtpPath = "";
				
				// Check if current FTP file is a directory or a real file
				if (ftpFile.isDirectory()) {
					// If it is a directory, get the name of it
					String currentDirName = ftpFile.getName();
					
					// Create a new relative path that can be added to the base path and check
					// the directory for files by calling this method again.
					relativeFtpPath += (remoteRelativePath.isEmpty()) ? currentDirName : remoteRelativePath + File.separator + currentDirName;
					getRemoteFiles(ftpClient, remoteBasePath, relativeFtpPath, remoteFileNames);
				} else {
					// If it is a file, get it's name and add it to a list
					String fileName = ftpFile.getName();			
					String remoteFileName = (remoteRelativePath.isEmpty()) ? fileName : remoteRelativePath + File.separator + fileName;
					remoteFileNames.add(remoteFileName);
				}
			}
		} catch (IOException e) {
			System.err.println("Error while getting remote files");
			e.printStackTrace();
		}
		
		return remoteFileNames;
	}


	public List<String> getDownloadedFiles() {
		return this.downloadedFiles;
	}
	
	
}
