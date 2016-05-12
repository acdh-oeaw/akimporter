package main.java.betullam.akimporter.main;
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


		// Get "from" timestamp:
		long fromTimeStamp = 0;
		try {
			String fromDateTime = "2016-01-13T14:00:00Z"; // Date from eMail from DNB: 13.01.2016, 14:00
			DateFormat fromDateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date date = fromDateTimeFormat.parse(fromDateTime);
			fromTimeStamp = date.getTime();
			System.out.println("From timestamp " + fromTimeStamp);

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		// First use current time as "until" time:
		String currentDateTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date(currentTimeStamp));
		
		// Difference between from and until
		long timeSpan = currentTimeStamp - fromTimeStamp;

		if (httpResponseCode == 200) {

			do {
				counter++;
				resumptionToken--;

				System.out.println("Resumption Token is " + resumptionToken + ". Counter is: " + counter);

			} while (resumptionToken != 0);

		} else if (httpResponseCode == 413) {
			//System.err.println("Too many documents requested from OAI interface (> 100000) . Please specify \"until\" date for OAI harvesting.");
			helperCounterForTest++;
			
			// Calculate new timestamp
			long newTimeStamp = (long)(currentTimeStamp - (timeSpan*0.10));
			System.err.println("Trying with new timestamp " + newTimeStamp);

			whileTest(newTimeStamp);
		} else {
			System.err.println("Response code is " + httpResponseCode);
		}
	}

}
