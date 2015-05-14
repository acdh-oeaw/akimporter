package betullam.akimporter.updater;

public class FtpFilename {
	private String dirName;
	private String fileName;
	

	public FtpFilename(String dirName, String fileName) {
		this.dirName = dirName;
		this.fileName = fileName;
	}

	public String getDirName() {
		return dirName;
	}

	public void setDirName(String dirName) {
		this.dirName = dirName;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
