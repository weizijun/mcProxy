package com.netease.backend.nkv.client.error;

public class NkvQueueOverflow extends NkvException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NkvQueueOverflow() {
		super();
	}

	public NkvQueueOverflow(String message, Throwable cause) {
		super(message, cause);
	}

	public NkvQueueOverflow(String message) {
		super(message);
	}

	public NkvQueueOverflow(Throwable cause) {
		super(cause);
	}
}
