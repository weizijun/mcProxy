package com.netease.backend.nkv.client.error;

public class NkvServerReject extends NkvException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NkvServerReject() {
		super();
	}

	public NkvServerReject(String message, Throwable cause) {
		super(message, cause);
	}

	public NkvServerReject(String message) {
		super(message);
	}

	public NkvServerReject(Throwable cause) {
		super(cause);
	}
}
