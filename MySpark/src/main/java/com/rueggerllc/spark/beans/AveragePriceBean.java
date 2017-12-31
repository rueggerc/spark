package com.rueggerllc.spark.beans;

import java.io.Serializable;

public class AveragePriceBean implements Serializable {
	
	private int count;
	private double total;
	
	public AveragePriceBean(int count, double total) {
		this.count = count;
		this.total = total;
	}
	
	
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public double getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	
	public Double computeAverage() {
		return total/count;
	}
	
	public String toString() {
		StringBuilder buffer = new StringBuilder();
		buffer.append("AveragePriceBean.count: " + count);
		buffer.append("\nAveragePriceBean.total: " + total);
		return buffer.toString();
	}
	


}
