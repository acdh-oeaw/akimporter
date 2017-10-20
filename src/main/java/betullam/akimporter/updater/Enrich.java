package main.java.betullam.akimporter.updater;

import java.io.File;

public class Enrich {
	
	private String enrichName;
	private boolean enrichDownload;
	private String enrichFtpHost;
	private String enrichFtpPort;
	private String enrichFtpUser; 
	private String enrichFtpPass; 
	private String enrichRemotePath; 
	private String enrichLocalPath; 
	private boolean enrichUnpack; 
	private boolean enrichMerge; 
	private String enrichProperties; 
	private String enrichSolr;
	private boolean print;
	private boolean optimize;
	
	public Enrich(String enrichName, boolean enrichDownload, String enrichFtpHost, String enrichFtpPort,
			String enrichFtpUser, String enrichFtpPass, String enrichRemotePath, String enrichLocalPath,
			boolean enrichUnpack, boolean enrichMerge, String enrichProperties, String enrichSolr, boolean print,
			boolean optimize) {
		this.enrichName = enrichName;
		this.enrichDownload = enrichDownload;
		this.enrichFtpHost = enrichFtpHost;
		this.enrichFtpPort = enrichFtpPort;
		this.enrichFtpUser = enrichFtpUser;
		this.enrichFtpPass = enrichFtpPass;
		this.enrichRemotePath = enrichRemotePath;
		this.enrichLocalPath = enrichLocalPath;
		this.enrichUnpack = enrichUnpack;
		this.enrichMerge = enrichMerge;
		this.enrichProperties = enrichProperties;
		this.enrichSolr = enrichSolr;
		this.print = print;
		this.optimize = optimize;
		
		this.enrich();
	};
	
	private void enrich() {
		
		if (this.enrichDownload) {
			String enrichLocalPathTarGz = this.enrichLocalPath + File.separator + "downloaded";
			int enrichFtpPortInt = Integer.valueOf(this.enrichFtpPort);
			new FtpDownload().downloadFiles(this.enrichRemotePath, enrichLocalPathTarGz, this.enrichFtpHost, enrichFtpPortInt, this.enrichFtpUser, this.enrichFtpPass, this.print);
		}
	}

}
