package com.netease.backend.nkv.mcProxy.net;

import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;
import com.netease.backend.nkv.mcProxy.command.Command;

/**
 * @author hzweizijun 
 * @date 2015年6月5日 下午10:25:23
 */
public class QueryMsg {
	private NkvResultFuture<?> future;
	private boolean isDone;
	private boolean isError;
	private McProxyChannel channel;
	private Command command;
	
	public QueryMsg(NkvResultFuture<?> future, McProxyChannel channel, Command command) {
		this.future = future;
		this.channel = channel;
		this.command = command;
		isDone = false;
		isError = false;
	}
	
	public NkvResultFuture<?> getFuture() {
		return future;
	}

	public boolean isDone() {
		return isDone;
	}
	public void setDone(boolean isDone) {
		this.isDone = isDone;
	}
	public boolean isError() {
		return isError;
	}
	public void setError(boolean isError) {
		this.isError = isError;
	}

	public McProxyChannel getChannel() {
		return channel;
	}

	public Command getCommand() {
		return command;
	}

	public void setCommand(Command command) {
		this.command = command;
	}
}
