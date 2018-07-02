package main.java.betullam.akimporter.saveloans;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.configuration2.INIConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import main.java.betullam.akimporter.main.AkImporterHelper;


public class SaveLoans {

	String pathToVuFind = null;
	INIConfiguration iniConfig;
	INIConfiguration iniConfigAlma;
	boolean print = false;


	public SaveLoans (String pathToVuFind, boolean print) {

		// Path to VuFind for getting the config files
		this.pathToVuFind = pathToVuFind;
		this.print = print;

		// Get config files
		this.iniConfig = this.getIniConfig(getPathToVuFindConfigFile("config.ini"));
		this.iniConfigAlma = this.getIniConfig(getPathToVuFindConfigFile("Alma.ini"));

		// Save the loans
		this.save();
	}
	

	private void save() {
		// Get the mysql connection string from the config.ini
		String mySqlConnString = this.iniConfig.getSection("Database").getProperty("database").toString();

		// Get jdbc connection string out of the mysql connection string of the config.ini
		String jdbcConnectionString = getJdbcConnectionString(mySqlConnString);

		// Get credentials for database out of the mysql connection string of the config.ini
		String dbUsername = null;
		String dbPassword = null;
		HashMap<String, String> dbCredentials = getDbCredentials(mySqlConnString);
		if (dbCredentials != null) {
			dbUsername = dbCredentials.get("dbUsername");
			dbPassword = dbCredentials.get("dbPassword");
		} else {
			System.err.print("Error while saving loans. No DB credentials found.");
			return;
		}

		// Get a database connection
		Connection connection = getDatabaseConnection(jdbcConnectionString, dbUsername, dbPassword, false, true);

		// Get a list of users that want to save their loans
		ArrayList<User> usersForSave = getUsersForSave(connection);

		// List for holding all loans
		ArrayList<Loan> allLoans = new ArrayList<Loan>();

		// Get loans from user
		for (User userForSave : usersForSave) {
			ArrayList<Loan> userLoans = this.getUserLoans(userForSave);
			
			AkImporterHelper.print(this.print, "\nGetting loans via API from user " + userForSave.getCatUsername());

			// Add loans from user to the list of all loans from all users
			if (userLoans != null) {
				allLoans.addAll(userLoans);
			}
		}

		if (!allLoans.isEmpty()) {
			// Add or update all loans in the loans database of VuFind
			this.saveLoansToDb(connection, allLoans);
		}

		try {
			connection.close();
		} catch (SQLException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		}
	}


	private void saveLoansToDb(Connection connection, ArrayList<Loan> loans) {

		for(Loan loan : loans) {
			//Statement stmt;
			PreparedStatement pstmt;
			try {
				
				// Query for inserting or updating (if exists) the loans table
				String query = "INSERT INTO loans"
						+ " (ils_loan_id, user_id, ils_user_id, item_id, title, author, publication_year, loan_date, due_date, library, location_code, call_no, barcode)"	
						+ " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
						+ " ON DUPLICATE KEY UPDATE" 
						+ " ils_loan_id=?, user_id=?, ils_user_id=?, item_id=?, title=?, author=?, publication_year=?, loan_date=?, due_date=?, library=?, location_code=?, call_no=?, barcode=?";

				pstmt = connection.prepareStatement(query);
				
				// Prepared statement
				pstmt.setString(1, loan.getIlsLoanId());
				pstmt.setInt(2, loan.getUserId());
				pstmt.setString(3, loan.getIlsUserId());
				pstmt.setString(4, loan.getItemId());
				pstmt.setString(5, loan.getTitle());
				pstmt.setString(6, loan.getAuthor());
				pstmt.setString(7, loan.getPublicationYear());
				pstmt.setString(8, loan.getLoanDate());
				pstmt.setString(9, loan.getDueDate());
				pstmt.setString(10, loan.getLibrary());
				pstmt.setString(11, loan.getLocationCode());
				pstmt.setString(12, loan.getCallNo());
				pstmt.setString(13, loan.getBarcode());
				pstmt.setString(14, loan.getIlsLoanId());
				pstmt.setInt(15, loan.getUserId());
				pstmt.setString(16, loan.getIlsUserId());
				pstmt.setString(17, loan.getItemId());
				pstmt.setString(18, loan.getTitle());
				pstmt.setString(19, loan.getAuthor());
				pstmt.setString(20, loan.getPublicationYear());
				pstmt.setString(21, loan.getLoanDate());
				pstmt.setString(22, loan.getDueDate());
				pstmt.setString(23, loan.getLibrary());
				pstmt.setString(24, loan.getLocationCode());
				pstmt.setString(25, loan.getCallNo());
				pstmt.setString(26, loan.getBarcode());

				pstmt.execute();
				
				AkImporterHelper.print(this.print, "\nSaved loan to database. Item ID: " + loan.getItemId() + ", Title: " + loan.getTitle());
				
			} catch (SQLException e) {
				System.err.print("Error while saving loans.");
				e.printStackTrace();
			}
		}

	}


	private ArrayList<Loan> getUserLoans(User user) {
		ArrayList<Loan> returnList = new ArrayList<Loan>();
		LinkedHashSet<String> urlParameters = new LinkedHashSet<String>();
		int vuFindUserId = user.getId();
		String ilsUserId = user.getCatId();

		urlParameters.add("users");
		urlParameters.add(ilsUserId);
		urlParameters.add("loans");
		Document result = this.apiRequest(urlParameters, null);

		if (result != null) {
			Element documentElement = result.getDocumentElement();
			NodeList nodeList = documentElement.getChildNodes();
			for (int i = 0; i < nodeList.getLength(); i++) {
				Node currentNode = nodeList.item(i);
				if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
					if (currentNode.getNodeName().equals("item_loan")) {

						// Create new loan object
						Loan loan = new Loan();
						loan.setUserId(vuFindUserId);

						// Iterate over child nodes from loan
						NodeList itemLoanNodes = currentNode.getChildNodes();
						for (int j = 0; j < itemLoanNodes.getLength(); j++) {
							if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
								Element element = (Element)itemLoanNodes.item(j);
								if (element.getTagName().equals("loan_id")) { loan.setIlsLoanId(element.getTextContent()); };
								if (element.getTagName().equals("user_id")) { loan.setIlsUserId(element.getTextContent()); };
								if (element.getTagName().equals("mms_id")) { loan.setItemId(element.getTextContent()); };
								if (element.getTagName().equals("title")) { loan.setTitle(element.getTextContent()); };
								if (element.getTagName().equals("author")) { loan.setAuthor(element.getTextContent()); };
								if (element.getTagName().equals("publication_year")) { loan.setPublicationYear(element.getTextContent()); };
								if (element.getTagName().equals("loan_date")) { loan.setLoanDate(element.getTextContent()); };
								if (element.getTagName().equals("due_date")) { loan.setDueDate(element.getTextContent()); };
								if (element.getTagName().equals("library")) { loan.setLibrary(element.getTextContent()); };
								if (element.getTagName().equals("location_code")) { loan.setLocationCode(element.getTextContent()); };
								if (element.getTagName().equals("call_number")) { loan.setCallNo(element.getTextContent()); };
								if (element.getTagName().equals("item_barcode")) { loan.setBarcode(element.getTextContent()); };
							}
						}

						// Add loan to a list of loans
						returnList.add(loan);
					}
				}
			}
		}

		return (returnList.isEmpty()) ? null : returnList;
	}


	private Document apiRequest(LinkedHashSet<String> urlParameters, ArrayList<NameValuePair> queryParameters) {
		Document doc = null;
		String apiUrl = (this.iniConfigAlma.getSection("API").getString("url") != null && !this.iniConfigAlma.getSection("API").getString("url").isEmpty()) ? AkImporterHelper.stripFileSeperatorFromPath(this.iniConfigAlma.getSection("API").getString("url")) : null;
		String apiKey = (this.iniConfigAlma.getSection("API").getString("key") != null && !this.iniConfigAlma.getSection("API").getString("key").isEmpty()) ? this.iniConfigAlma.getSection("API").getString("key") : null;
		if (apiUrl == null || apiKey == null) {
			System.err.println("API URL or Key is empty. See Alma.ini of VuFind, section [API], configuration name \"url\" and \"key\".");
			return null;
		}
		queryParameters = (queryParameters == null) ? new ArrayList<NameValuePair>() : queryParameters;

		// Add the API Key to the query parameters
		queryParameters.add(new BasicNameValuePair("apikey", apiKey));

		URL url;
		HttpURLConnection httpUrlConnection = null;
		try {
			if (urlParameters != null) {
				for (String urlParameter : urlParameters) {
					apiUrl += "/" + urlParameter;
				}
			}

			// Add trailing slash
			apiUrl += "/";

			String queryParameterString = (queryParameters != null) ? URLEncodedUtils.format(queryParameters, StandardCharsets.UTF_8) : "";
			url = new URL(apiUrl + "?" + queryParameterString);
			httpUrlConnection = (HttpURLConnection) url.openConnection();
			httpUrlConnection.setRequestMethod("GET");
			httpUrlConnection.setRequestProperty("Accept", "application/xml");
			InputStream xml = null;

			if (httpUrlConnection.getResponseCode() == 200) {
				xml = httpUrlConnection.getInputStream();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
				DocumentBuilder db = dbf.newDocumentBuilder();
				doc = db.parse(xml);
			}
		} catch (MalformedURLException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		} catch (SAXException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		} finally {
			httpUrlConnection.disconnect();
		}

		return doc;
	}


	private ArrayList<User> getUsersForSave(Connection connection) {
		ArrayList<User> usersForSave = new ArrayList<User>();
		Statement stmt;
		try {
			stmt = connection.createStatement();

			// Get all "cat_username"s that want to save their loans
			ResultSet rs = stmt.executeQuery("SELECT id, cat_username, cat_id FROM user WHERE save_loans=1");

			while (rs.next()) {
				User user = new User(rs.getInt("id"), rs.getString("cat_username"), rs.getString("cat_id"));
				usersForSave.add(user);
			}

		} catch (SQLException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		}

		return usersForSave;
	}


	private Connection getDatabaseConnection(String jdbcConnectionString, String dbUsername, String dbPassword, boolean verifyServerCertificate, boolean useSSL) {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:" + jdbcConnectionString + "?user=" + dbUsername + "&password=" + dbPassword + "&verifyServerCertificate=" + ((verifyServerCertificate) ? "true" : "false") + "&useSSL=" + ((useSSL) ? "true" : "false"));
		} catch (SQLException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		}
		return connection;
	}


	private String getJdbcConnectionString(String mySqlConnString) {
		String jdbcConnectionString = "";

		// Pattern for matching the username and password
		String regExJdbcConnectionString = "(^.*?\\/\\/)(.*)@(.*?$)";

		// Create pattern and matcher
		Pattern pattern = Pattern.compile(regExJdbcConnectionString);
		Matcher matcher = pattern.matcher(mySqlConnString);

		if (matcher.find()) {
			// Use group 0 and 2. Group 1 contains username and password which we don't need here
			jdbcConnectionString += matcher.group(1);
			jdbcConnectionString += matcher.group(3);
		}

		if (jdbcConnectionString == "") {
			jdbcConnectionString = null;
			System.err.println("Cannot find propper database connection string. See config.ini of VuFind, section [Database], configuration name \"database\". It should look like this: \"mysql://dbuser:dbpass@host:port/database\"");
		}

		return jdbcConnectionString;
	}


	private HashMap<String, String> getDbCredentials(String mySqlConnString) {
		HashMap<String, String> returnValue = null;

		// Pattern for matching the username and password
		String regExUsernamePassword = "(?<=.*?:\\/\\/).*(?=@)";

		// Create pattern and matcher
		Pattern pattern = Pattern.compile(regExUsernamePassword);
		Matcher matcher = pattern.matcher(mySqlConnString);

		String usernamePassword = "";
		while(matcher.find()) {
			usernamePassword += matcher.group();
		}

		if (usernamePassword != "") {
			String[] usernamePasswordArr = usernamePassword.split(":", 2);
			returnValue = new HashMap<String, String>();
			returnValue.put("dbUsername", usernamePasswordArr[0]);
			try {
				returnValue.put("dbPassword", URLEncoder.encode(usernamePasswordArr[1], "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				System.err.print("Error while saving loans.");
				e.printStackTrace();
			}
		} else {
			System.err.println("Cannot find username and password in database connection string. See config.ini of VuFind, section [Database], configuration name \"database\". It should look like this: \"mysql://dbuser:dbpass@host:port/database\"");
		}

		return returnValue;
	}


	private String getPathToVuFindConfigFile(String configFileName) {
		String pathToVuFindConfigFile = null;

		String pathToVuFind = (this.pathToVuFind != null && !this.pathToVuFind.isEmpty()) ? AkImporterHelper.stripFileSeperatorFromPath(this.pathToVuFind) : null;
		if (pathToVuFind == null) {
			System.err.println("Set the path to your VuFind installation as the parameter of \"save_loans\"");
			return null;
		}

		String localConfigFilePath = pathToVuFind + File.separator + "local" + File.separator + "config"  + File.separator + "vufind" + File.separator + configFileName;

		File configFile = new File(localConfigFilePath);
		if (!configFile.exists()) {
			String defaultConfigFilePath = pathToVuFind + File.separator + "config"  + File.separator + "vufind" + File.separator + configFileName;
			configFile = new File(defaultConfigFilePath);
			if (!configFile.exists()) {
				System.err.println("Config file " + configFile.getAbsolutePath() + " does not exist!");
				return null;
			}
		}

		pathToVuFindConfigFile = configFile.getAbsolutePath();
		return pathToVuFindConfigFile;
	}


	private INIConfiguration getIniConfig(String pathToIni) {
		INIConfiguration iniConfig = null;
		File iniConfigFile = new File(pathToIni);
		if (!iniConfigFile.exists()) {
			System.err.println("Config file " + pathToIni + " does not exist!");
			return null;
		}

		Configurations configs = new Configurations();
		try {
			iniConfig = configs.ini(iniConfigFile);
		} catch (ConfigurationException e) {
			System.err.print("Error while saving loans.");
			e.printStackTrace();
		}

		return iniConfig;
	}
	
}