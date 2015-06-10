package com.netease.backend.nkv.mcProxy;

import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.net.McProxyChannel;

/**
 * @author hzweizijun 
 * @date 2015年6月10日 下午2:48:56
 */
public class QueryOneMsg extends QueryMsg {
	private NkvFuture future;

	public QueryOneMsg(NkvResultFuture<?> future, McProxyChannel channel,
			Command command) {
		super(future, channel, command);
	}
	
	public boolean isDone() {
		return isDone;
	}
	public void setDone(NkvFuture future,boolean isDone) {
		this.isDone = isDone;
	}

	@Override
	public void addFuture(NkvFuture future) {
		this.future = future;
	}
}
