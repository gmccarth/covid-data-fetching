package com.redhat;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.camel.dataformat.bindy.annotation.CsvRecord;
import org.apache.camel.dataformat.bindy.annotation.DataField;

@XmlRootElement(name="jhu-csse-report")
@CsvRecord(separator = ",", skipField=true, quote="\"")
public class JhuCsseDailyReportCsvRecordv2 {
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
