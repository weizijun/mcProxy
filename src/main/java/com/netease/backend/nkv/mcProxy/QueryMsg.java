package com.netease.backend.nkv.mcProxy;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.net.McProxyChannel;

/**
 * @author hzweizijun 
 * @date 2015年6月5日 下午10:25:23
 */
public abstract class QueryMsg {
	protected NkvResultFuture<?> resultFuture;
	protected boolean isDone;
	protected boolean isError;
	protected McProxyChannel channel;
	protected Command command;
	public final static ChannelBuffer ErrorBuffer = ChannelBuffers.copiedBuffer("ERROR\r\n".getBytes());
	
	public QueryMsg(NkvResultFuture<?> future, McProxyChannel channel, Command command) {
		this.resultFuture = future;
		this.channel = channel;
		this.command = command;
		isDone = false;
		isError = false;
	}
	
	public NkvResultFuture<?> getFuture() {
		return resultFuture;
	}

	public abstract boolean isDone();
	public abstract void setDone(NkvFuture future, boolean isDone);
	public abstract void addFuture(NkvFuture future);
	
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
}
