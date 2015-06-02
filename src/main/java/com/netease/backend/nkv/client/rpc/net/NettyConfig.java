package com.netease.backend.nkv.client.rpc.net;

public class NettyConfig {
	private short port;

	private int bossThreadCount;

	private int workerThreadCount;

	private boolean tcpNoDelay;

	private boolean keepAlive;
	
	private int keepAliveTimeoutSeconds;

	public short getPort() {
		return port;
	}

	public void setPort(short port) {
		this.port = port;
	}

	public int getBossThreadCount() {
		return bossThreadCount;
	}

	public void setBossThreadCount(int bossThreadCount) {
		this.bossThreadCount = bossThreadCount;
	}

	public int getWorkerThreadCount() {
		return workerThreadCount;
	}

	public void setWorkerThreadCount(int workerThreadCount) {
		this.workerThreadCount = workerThreadCount;
	}

	public boolean isTcpNoDelay() {
		return tcpNoDelay;
	}

	public void setTcpNoDelay(boolean tcpNoDelay) {
		this.tcpNoDelay = tcpNoDelay;
	}

	public boolean isKeepAlive() {
		return keepAlive;
	}

	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public int getKeepAliveTimeoutSeconds() {
		return keepAliveTimeoutSeconds;
	}

	public void setKeepAliveTimeoutSeconds(int keepAliveTimeoutSeconds) {
		this.keepAliveTimeoutSeconds = keepAliveTimeoutSeconds;
	}

}
