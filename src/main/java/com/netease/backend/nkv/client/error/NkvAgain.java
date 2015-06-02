package com.netease.backend.nkv.client.error;

public class NkvAgain extends NkvException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NkvAgain() {
		super();
	}

	public NkvAgain(String message, Throwable cause) {
		super(message, cause);
	}

	public NkvAgain(String message) {
		super(message);
	}

	public NkvAgain(Throwable cause) {
		super(cause);
	}
	
}
