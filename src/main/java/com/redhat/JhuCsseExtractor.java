package com.redhat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.bindy.annotation.CsvRecord;
import org.apache.camel.dataformat.bindy.annotation.DataField;
import org.apache.camel.model.dataformat.BindyType;
import org.apache.camel.model.dataformat.JsonLibrary;

public class JhuCsseExtractor extends RouteBuilder {

	
	@Override
  public void configure() throws Exception {


		from("timer:jhucsse?repeatCount=1")
				.setHeader("nextFile", method(this, "computeNextFile"))
				.loopDoWhile(method(this, "someMethodNameHere"))
				.toD("${header.nextFile}")
				.split().tokenize("\n", 1, true)
				.unmarshal().bindy(BindyType.Csv, JhuCsseDailyReportCsvRecordv1.class)
				.marshal().json(JsonLibrary.Jackson)
				.to("kafka:jhucsse");

	}
	
	public void computeNextFile() {
		String startDateStr = "02-01-2020";
		String endDateStr = "03-22-2020";
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");
		LocalDate date = LocalDate.parse(startDateStr, dtf);
		LocalDate endDate = LocalDate.parse(endDateStr, dtf);
	}
  
	@XmlRootElement(name="jhu-csse-report")
	@CsvRecord(separator = ",", skipField=true, quote="\"")
	static class JhuCsseDailyReportCsvRecordv2 {



		@DataField(pos = 2)
		private String admin2;



		@DataField(pos = 3)
		private String provinceState;

		@DataField(pos = 4)
		private String countryRegion;

		@DataField(pos = 5)
		private String lastUpdate;

		@DataField(pos = 8)
		private int confirmedCases;

		@DataField(pos = 9)
		private int deaths;
		
		private Long reportDate;

		public String getAdmin2() {
			return admin2;
		}

		public void setAdmin2(String admin2) {
			this.admin2 = admin2;
		}

		public String getProvinceState() {
			return provinceState;
		}

		public void setProvinceState(String provinceState) {
			this.provinceState = provinceState;
		}

		@XmlElement(name = "country")
		public String getCountryRegion() {
			return countryRegion;
		}

		public void setCountryRegion(String countryRegion) {
			this.countryRegion = countryRegion;
		}

		public int getConfirmedCases() {
			return confirmedCases;
		}

		public void setConfirmedCases(int confirmedCases) {
			this.confirmedCases = confirmedCases;
		}

		public int getDeaths() {
			return deaths;
		}

		public void setDeaths(int deaths) {
			this.deaths = deaths;
		}

		public Long getReportDate() {
			Long epoch = 0L;
			try {
				epoch = new SimpleDateFormat("MM-dd-yyyy").parse(getLastUpdate().substring(0,10)).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return epoch;
		}

		public String getLastUpdate() {
			return lastUpdate;
		}

		public void setLastUpdate(String lastUpdate) {
			this.lastUpdate = lastUpdate;
		}

		public void setReportDate(String lastUpdate) {
			this.lastUpdate = lastUpdate;
		}
	}
	
	@XmlRootElement(name="jhu-csse-report")
	@CsvRecord(separator = ",", skipField=true, quote="\"")
	static class JhuCsseDailyReportCsvRecordv1 {

		@DataField(pos = 1)
		private String provinceState;

		@DataField(pos = 2)
		private String countryRegion;

		@DataField(pos = 3)
		private String lastUpdate;

		@DataField(pos = 4)
		private int confirmedCases;

		@DataField(pos = 5)
		private int deaths;
		
		private Long reportDate;


		//    public String getAdmin2() {
		//      return admin2;
		//    }

		//    public void setAdmin2(String admin2) {
		//      this.admin2 = admin2;
		//    }

		public String getProvinceState() {
			return provinceState;
		}

		public void setProvinceState(String provinceState) {
			this.provinceState = provinceState;
		}

		@XmlElement(name = "country")
		public String getCountryRegion() {
			return countryRegion;
		}

		public void setCountryRegion(String countryRegion) {
			this.countryRegion = countryRegion;
		}

		public void setReportDate(String lastUpdate) {
			this.lastUpdate = lastUpdate;
		}

		public int getConfirmedCases() {
			return confirmedCases;
		}

		public void setConfirmedCases(int confirmedCases) {
			this.confirmedCases = confirmedCases;
		}

		public int getDeaths() {
			return deaths;
		}

		public void setDeaths(int deaths) {
			this.deaths = deaths;
		}

		public Long getReportDate() {
			Long epoch = 0L;
			try {
				epoch = new SimpleDateFormat("MM-dd-yyyy").parse(getLastUpdate().substring(0,10)).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return epoch;
		}

		public String getLastUpdate() {
			return lastUpdate;
		}

		public void setLastUpdate(String lastUpdate) {
			this.lastUpdate = lastUpdate;
		}


	}
  
}