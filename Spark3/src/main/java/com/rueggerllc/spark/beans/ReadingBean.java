package com.rueggerllc.spark.beans;

import java.sql.Timestamp;

public class ReadingBean {
	
	private String sensor_id;
	private String notes;
	private Timestamp reading_time;
	private double temperature;
	private double humidity;
	
	public String getSensor_id() {
		return sensor_id;
	}
	public void setSensor_id(String sensor_id) {
		this.sensor_id = sensor_id;
	}
	public String getNotes() {
		return notes;
	}
	public void setNotes(String notes) {
		this.notes = notes;
	}
	public double getTemperature() {
		return temperature;
	}
	public void setTemperature(double temperature) {
		this.temperature = temperature;
	}
	public double getHumidity() {
		return humidity;
	}
	public void setHumidity(double humidity) {
		this.humidity = humidity;
	}
	public Timestamp getReading_time() {
		return reading_time;
	}
	public void setReading_time(Timestamp reading_time) {
		this.reading_time = reading_time;
	}

}
