package com.redhat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.camel.Header;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.BindyType;
import org.apache.camel.model.dataformat.JsonLibrary;

public class JhuCsseExtractor extends RouteBuilder {

	
	@Override
  public void configure() throws Exception {


		from("timer:jhucsse?repeatCount=1")
			.setHeader("nextFile", simple("02-01-2020"))
			.setHeader("version", simple("v1"))
			.loopDoWhile(method(this, "dateInValidRange(${header.nextFile})"))
				.setHeader("nextFile", method(this, "computeNextFile(${header.nextFile})"))
				.setHeader("version", method(this, "getVersion(${header.nextFile})"))
				.toD("https:{{jhu.csse.baseUrl}}/${header.nextFile}.csv?httpMethod=GET")
				.log(LoggingLevel.DEBUG,"after setHeader:nextFile=${header.nextFile}")
				.split().tokenize("\n", 1, true)
				.log(LoggingLevel.DEBUG,"version=${header.version}")
				.choice()
					.when(header("version").isEqualTo("v1"))
						.unmarshal().bindy(BindyType.Csv, JhuCsseDailyReportCsvRecordv1.class)
						.marshal().json(JsonLibrary.Jackson)
						.to("kafka:jhucsse")
					.otherwise()
						.unmarshal().bindy(BindyType.Csv, JhuCsseDailyReportCsvRecordv2.class)
						.marshal().json(JsonLibrary.Jackson)
						.to("kafka:jhucsse")
			.end();

	}
	
	public String computeNextFile(@Header("nextFile") String oldDate) {
		
		if(oldDate==null) oldDate = "02-02-2020";
		System.out.println("oldDate:" + oldDate);
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");
		LocalDate date = LocalDate.parse(oldDate, dtf);
		LocalDate nextDay = date.plusDays(1);
		String nextDate = nextDay.format(dtf);
		System.out.println("nextDate = " + nextDate);
		
		return nextDate;
	}
	
	public Boolean dateInValidRange(@Header("nextFile") String dateToCheck) {
		
		if(dateToCheck==null) dateToCheck = "02-02-2020";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");
		LocalDate date = LocalDate.parse(dateToCheck, dtf);
		LocalDate endDate = LocalDate.parse("05-21-2020", dtf);
		Boolean validDate = date.isBefore(endDate);
		System.out.println("Date in valid range: " + date.isBefore(endDate) + " > " + dateToCheck);
		return validDate;
	}
  
	public String getVersion(@Header("nextFile") String dateToCheck) {
		
		String version = "v1";
		if(dateToCheck==null) dateToCheck = "02-02-2020";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");
		LocalDate date = LocalDate.parse(dateToCheck, dtf);
		LocalDate changeDate = LocalDate.parse("03-21-2020", dtf);
		if(date.isAfter(changeDate)) version="v2";
		System.out.println("Date:" + dateToCheck + " > " + version);
		return version;
	}
	
	
  
}