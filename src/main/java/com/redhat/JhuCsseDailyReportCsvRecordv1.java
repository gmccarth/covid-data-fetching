package com.redhat;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.camel.dataformat.bindy.annotation.CsvRecord;
import org.apache.camel.dataformat.bindy.annotation.DataField;

@XmlRootElement(name="jhu-csse-report")
@CsvRecord(separator = ",", skipField=true, quote="\"")
public class JhuCsseDailyReportCsvRecordv1 {

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
		return provinceState.toLowerCase();
	}

	public void setProvinceState(String provinceState) {
		this.provinceState = provinceState;
	}

	@XmlElement(name = "country")
	public String getCountryRegion() {
		return countryRegion.toLowerCase();
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
			epoch = new SimpleDateFormat("yyyy-MM-dd").parse(getLastUpdate()).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return epoch;
	}
	
	public void setReportDate(Long reportDate) {
		this.reportDate = reportDate;
	}

	public String getLastUpdate() {
		String lastUpdateTrunc = lastUpdate.split(" ")[0].length() > 10 ? lastUpdate.split("T")[0] : lastUpdate.split(" ")[0];
		
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("M/dd/yy");
		
		int length = lastUpdateTrunc.length();
		try {
			lastUpdate = (length<10) ? sdf1.format(sdf2.parse(lastUpdateTrunc)) : lastUpdateTrunc;
	
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lastUpdate;
	}

	public void setLastUpdate(String lastUpdate) {
		this.lastUpdate = lastUpdate;
	}


}


