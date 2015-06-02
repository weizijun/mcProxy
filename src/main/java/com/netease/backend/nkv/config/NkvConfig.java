package com.netease.backend.nkv.config;
/**
 * @author hzweizijun 
 * @date 2015年6月2日 下午1:43:26
 */
public class NkvConfig {
	private String master;
	private String slave;
	private String group;
	
	public String getMaster() {
		return master;
	}
	public void setMaster(String master) {
		this.master = master;
	}
	public String getSlave() {
		return slave;
	}
	public void setSlave(String slave) {
		this.slave = slave;
	}
	public String getGroup() {
		return group;
	}
	public void setGroup(String group) {
		this.group = group;
	}
}
