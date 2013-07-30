package jcascalog.parquet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ElectricPowerUsage implements Serializable {
	
	private static final long serialVersionUID = -2095862233077409553L;
	
	private List<DevicePowerEvent> devicePowerEventList = new ArrayList<DevicePowerEvent>();
	
	/**
	 * <시도><시군구><동면><번지>-<상세주소> 를 2자리 3자리 3자리 3자리(표준주소체계)-4자리 형태의 Code.
	 */
	private String addressCode;
	
	private long timestamp;
	
	public List<DevicePowerEvent> getDevicePowerEventList() {
		return devicePowerEventList;
	}
	
	public void addDevicePowerEvent(DevicePowerEvent devicePowerEvent) {
		this.devicePowerEventList.add(devicePowerEvent);
	}
	
	public void setDevicePowerEventList(List<DevicePowerEvent> devicePowerEventList) {
		this.devicePowerEventList = devicePowerEventList;
	}
	
	public String getAddressCode() {
		return addressCode;
	}
	
	public void setAddressCode(String addressCode) {
		this.addressCode = addressCode;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
}
