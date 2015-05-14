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
