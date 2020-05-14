package com.redhat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.camel.Header;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.BindyType;
import org.apache.camel.model.dataformat.JsonLibrary;

public class JhuCsseExtractor extends RouteBuilder {

	
	@Override
  public void configure() throws Exception {


		from("timer:jhucsse?repeatCount=1")
				.setHeader("nextFile", method(this, "computeNextFile"))
				.loopDoWhile(method(this, "dateInValidRange"))
				.toD("https:{{jhu.csse.baseUrl}}/${header.nextFile}.csv")
				.log("nextFile=${header.nextFile}")
				.split().tokenize("\n", 1, true)
				.unmarshal().bindy(BindyType.Csv, JhuCsseDailyReportCsvRecordv1.class)
				.marshal().json(JsonLibrary.Jackson)
				.to("kafka:jhucsse");

	}
	
	public String computeNextFile(String oldDate) {
		
		if(oldDate==null) oldDate = "02-01-2020";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");
		LocalDate date = LocalDate.parse(oldDate, dtf);
		LocalDate nextDay = date.plusDays(1);
		String nextDate = nextDay.format(dtf);
		System.out.println("nextDate = " + nextDate);
		
		return nextDate;
	}
	
	public boolean dateInValidRange(@Header("nextFile") String dateToCheck) {
		
		
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");
		LocalDate date = LocalDate.parse(dateToCheck, dtf);
		LocalDate endDate = LocalDate.parse("03-23-2020", dtf);
		System.out.println("Date in valid range: " + date.isBefore(endDate));
		return date.isBefore(endDate);
	}
  

	
	
  
}