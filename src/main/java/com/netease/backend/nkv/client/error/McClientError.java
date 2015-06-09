package com.netease.backend.nkv.client.error;
/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午4:46:37
 */
public class McClientError extends McError {

	/**
	 * 
	 */
	private static final long serialVersionUID = 659728870674551034L;

	public McClientError() {
		super();
	}

	public McClientError(String message, Throwable cause) {
		super(message, cause);
	}

	public McClientError(String message) {
		super(message);
	}

	public McClientError(Throwable cause) {
		super(cause);
	}
	
	@Override
	public String getMessage() {
		return "CLIENT_ERROR" + (message == null ? "" : " " + super.getMessage()) + "\r\n";
	}
}
