package betullam.akimporter.updater;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class FtpDownload_V1 {

	static List<FtpFilename> fileList = new ArrayList<FtpFilename>();

	public boolean downloadFiles(String remotePath, String localPath, String host, int port, String user, String password, boolean isBinary, boolean showMessages) {
		boolean ftpOk = false;
		FTPClient ftpClient = new FTPClient();

		try {
			ftpClient.connect( host, port );
			ftpClient.enterLocalPassiveMode();
			if (isBinary == true) {
				ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
			}
			if( showMessages ) { System.out.println( ftpClient.getReplyString() ); }
			ftpOk &= ftpClient.login( user, password );
			if( showMessages ) { System.out.println( ftpClient.getReplyString() ); }

			List<FtpFilename> ftpFilenames = getFileNames(ftpClient, remotePath, "", 0);
			for (FtpFilename ftpFilename : ftpFilenames) {
				String dirName = (ftpFilename.getDirName() != null && !ftpFilename.getDirName().equals("")) ? ftpFilename.getDirName() + File.separator : "";

				String fileName = ftpFilename.getFileName();
				File downloadFile = new File(localPath + File.separator + fileName);
				OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
				boolean success = ftpClient.retrieveFile(dirName + fileName, outputStream);
				outputStream.close();

				if (showMessages) { 
					if (success) {
						System.out.println("Successfully downloaded file \"" + fileName + "\"");
						ftpOk = true;
					} else {
						ftpOk = false;
						System.out.println("ERROR downloading file from FTP-Server!");
					}
				}

			}

			ftpClient.logout();
			if( showMessages ) { System.out.println( ftpClient.getReplyString() ); }
			ftpClient.disconnect();
			if( showMessages ) { System.out.println( "FTP Client disconnected" ); }
		} catch (IOException e) {
			ftpOk = false;
			e.printStackTrace();
		}
		return ftpOk;
	}

	
	
	
	// Get filenames also from subdirectories - we don't need that with the current setup:
	// Code from: http://www.codejava.net/java-se/networking/ftp/list-files-and-directories-recursively-on-a-ftp-server
	static List<FtpFilename> getFileNames(FTPClient ftpClient, String parentDir, String currentDir, int level) throws IOException {

		String dirToList = parentDir;
		if (!currentDir.equals("")) {
			dirToList += "/" + currentDir;
		}

		FTPFile[] subFiles = ftpClient.listFiles(dirToList);
		if (subFiles != null && subFiles.length > 0) {
			for (FTPFile ftpFile : subFiles) {
				String currentFileName = ftpFile.getName();
				//fileList.add(currentFileName);

				if (currentFileName.equals(".") || currentFileName.equals("..")) {
					// skip parent directory and directory itself
					continue;
				}
				for (int i = 0; i < level; i++) {
					//System.out.print("\t");
				}
				if (ftpFile.isDirectory()) {
					//System.out.println("[" + currentFileName + "]");
					getFileNames(ftpClient, dirToList, currentFileName, level + 1);
				} else {
					fileList.add(new FtpFilename(dirToList, currentFileName));
					//System.out.println(dirToList + File.separator + currentFileName);
				}

			}
		}

		return fileList;
	}
	
	


}
