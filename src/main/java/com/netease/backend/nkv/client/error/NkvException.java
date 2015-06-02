package com.netease.backend.nkv.client.error;

import java.net.SocketAddress;

public class NkvException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NkvException() {
		super();
	}

	 
	public NkvException(String message, Throwable cause) {
		super(message, cause);
	}

	public NkvException(String message) {
		super(message);
	}

	public NkvException(Throwable cause) {
		super(cause);
	}

}
