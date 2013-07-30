package jcascalog.parquet;

import java.io.Serializable;

public class DevicePowerEvent implements Serializable {
	
	private static final long serialVersionUID = -1144491814069529604L;
	
	private double power;
	
	/**
	 * ex) 1:에어컨, 2:선풍기, 3:전기장판, 4:TV, 5:냉장고, ...
	 */
	private int deviceType;
	
	/**
	 * 동종 Device 가 2개이상일 경우 붙이는 ID.
	 * 하나 일 경우에는 0.
	 */
	private int deviceId = 0;
	
	/**
	 * 0: OFF,
	 * 1: ON.
	 */
	private int status;
	
	public double getPower() {
		return power;
	}
	
	public void setPower(double power) {
		this.power = power;
	}
	
	public int getDeviceType() {
		return deviceType;
	}
	
	public void setDeviceType(int deviceType) {
		this.deviceType = deviceType;
	}
	
	public int getDeviceId() {
		return deviceId;
	}
	
	public void setDeviceId(int deviceId) {
		this.deviceId = deviceId;
	}
	
	public int getStatus() {
		return status;
	}
	
	public void setStatus(int status) {
		this.status = status;
	}
}