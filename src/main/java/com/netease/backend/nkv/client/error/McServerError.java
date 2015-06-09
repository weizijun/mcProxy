package com.netease.backend.nkv.client.error;
/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午4:47:02
 */
public class McServerError extends McError {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4191702452872301894L;

	public McServerError() {
		super();
	}

	public McServerError(String message, Throwable cause) {
		super(message, cause);
	}

	public McServerError(String message) {
		super(message);
	}

	public McServerError(Throwable cause) {
		super(cause);
	}
	
	@Override
	public String getMessage() {
		return "SERVER_ERROR" + (message == null ? "" : " " + super.getMessage()) + "\r\n";
	}
}
