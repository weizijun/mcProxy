package com.netease.backend.nkv.client.error;

public class NkvRpcError extends NkvException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NkvRpcError() {
		super();
	}

	public NkvRpcError(String message, Throwable cause) {
		super(message, cause);
	}

	public NkvRpcError(String message) {
		super(message);
	}

	public NkvRpcError(Throwable cause) {
		super(cause);
	}
	
}
