package com.netease.backend.nkv.mcProxy;

import java.util.HashMap;
import java.util.Map;

import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.net.McProxyChannel;

/**
 * @author hzweizijun 
 * @date 2015年6月10日 下午2:50:07
 */
public class QueryMultiMsg extends QueryMsg {
	private Map<NkvFuture, Boolean> futureMap;
	private int futureCount;
	private int doneCount = 0;

	public QueryMultiMsg(NkvResultFuture<?> future, McProxyChannel channel,
			Command command) {
		super(future, channel, command);
		futureMap = new HashMap<NkvFuture, Boolean>();
	}
	
	public boolean isDone() {
		return isDone;
	}
	
	public void setDone(NkvFuture future,boolean isDone) {
		synchronized (resultFuture) {
			Boolean futureDone = futureMap.get(future);
			if (futureDone == null) {
				
			} else if (futureDone == false) {
				futureMap.put(future, true);
				doneCount++;
				
				if (doneCount == futureCount) {
					this.isDone = true;
				}
			}
		}
	}

	@Override
	public void addFuture(NkvFuture future) {
		futureMap.put(future, false);
		futureCount = futureMap.size();
	}
}
