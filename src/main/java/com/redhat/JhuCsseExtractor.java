package com.redhat;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.dataformat.bindy.annotation.CsvRecord;
import org.apache.camel.dataformat.bindy.annotation.DataField;
import org.apache.camel.model.dataformat.BindyType;
import org.apache.camel.model.dataformat.JsonLibrary;

public class JhuCsseExtractor extends RouteBuilder {
	  
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("MM-dd-yyyy");  
    LocalDateTime now = LocalDateTime.now();
//    LocalDateTime previous = LocalDateTime.now().minusDays(1);
    
//    String yesterday = dtf.format(previous);
//    String today = dtf.format(now);
    static String today = "05-06-2020";
    static String yyymmdd = today.substring(6,10).concat(today.substring(2,3)).concat(today.substring(0,2)).concat(today.substring(2,5));
    static SimpleDateFormat sdf = new SimpleDateFormat("yyy-MM-dd");
    
	
	@Override
  public void configure() throws Exception {



    from("timer:jhucsse?repeatCount=1")
    .toF("https:{{jhu.csse.baseUrl}}/%s.csv", today)
    .split().tokenize("\n", 1, true)
    .unmarshal().bindy(BindyType.Csv, JhuCsseDailyReportCsvRecord.class)
    .marshal().json(JsonLibrary.Jackson)
    .to("kafka:jhucsse");
  }

  @XmlRootElement(name="jhu-csse-report")
  @CsvRecord(separator = ",", skipField=true, quote="\"")
  public static class JhuCsseDailyReportCsvRecord {

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
    
//    @DataField(pos = 1)
//    private String provinceState;
//
//    @DataField(pos = 2)
//    private String countryRegion;
//
//    @DataField(pos = 3)
//    private String lastUpdate;
//
//	@DataField(pos = 4)
//	private int confirmedCases;
//
//    @DataField(pos = 5)
//    private int deaths;
    
    private Date reportDate;

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

    public String getLastUpdate() {
      return lastUpdate;
    }

    public void setLastUpdate(String lastUpdate) {
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
    
    public Date getReportDate() throws ParseException {
		return sdf.parse(yyymmdd);
	}

	public void setReportDate(Date reportDate) {
		this.reportDate = reportDate;
	}

  }
}