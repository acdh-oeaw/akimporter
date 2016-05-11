package main.java.betullam.akimporter.main;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WhileTest {

	private static int resumptionToken = 5;
	private static int counter = 0;
	private static int helperCounterForTest = 0;
	//private static long currentTimeStamp;

	private static int httpResponseCode;

	public static void main(String[] args) {

		long currentTimeStamp = new Date().getTime();
		System.out.println("Current timestamp is " + currentTimeStamp);

		whileTest(currentTimeStamp); // First start with current date/time


	}


	private static void whileTest(long currentTimeStamp) {

		if (helperCounterForTest < 5) {
			httpResponseCode = 413;
		} else {
			httpResponseCode = 200;
		}


		String currentDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date(currentTimeStamp));

		try {
			// Datum aus eMail from DNB: 13.01.2016, 14:00
			String fromDateTime = "2016-01-13T14:00:00Z";
			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date date = dateFormat.parse(fromDateTime);
			long fromTimestamp = date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if (httpResponseCode == 200) {

			do {
				counter++;
				resumptionToken--;

				System.out.println("Resumption Token is " + resumptionToken + ". Counter is: " + counter);

			} while (resumptionToken != 0);

		} else if (httpResponseCode == 413) {
			//System.err.println("Too many documents requested from OAI interface (> 100000) . Please specify \"until\" date for OAI harvesting.");
			helperCounterForTest++;
			
			long newTimeStamp = (long)(currentTimeStamp - (currentTimeStamp*0.15));
			System.err.println("Trying with new timestamp " + newTimeStamp);

			whileTest(newTimeStamp);
		} else {
			System.err.println("Response code is " + httpResponseCode);
		}
	}

}
