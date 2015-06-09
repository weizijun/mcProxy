package com.netease.backend.nkv.client.error;
/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午4:46:04
 */
public class McError extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7551131603130555451L;
	
	protected String message;

	public McError() {
		super();
	}

	public McError(String message, Throwable cause) {
		super(message, cause);
		this.message = message; 
	}

	public McError(String message) {
		super(message);
		this.message = message;
	}

	public McError(Throwable cause) {
		super(cause);
	}
	
	@Override
	public String getMessage() {
		return "ERROR" + (message == null ? "" : " " + super.getMessage()) + "\r\n";
	}
}
