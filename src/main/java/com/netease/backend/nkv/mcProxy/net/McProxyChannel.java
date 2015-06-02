package com.netease.backend.nkv.mcProxy.net;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;

/**
 * @author hzweizijun 
 * @date 2015年6月2日 下午4:15:56
 */
public class McProxyChannel {
	private Channel channelImpl = null;
	private final static AtomicInteger channelSeq ;
	private ConcurrentMap<Integer, NkvFuture> futureMap = new ConcurrentHashMap<Integer, NkvFuture>();
	
	static {
		channelSeq = new AtomicInteger(1);
	}
	
	public McProxyChannel(Channel channel) {
		channelImpl = channel;
	}
	
	public Integer putResult(NkvFuture future) {
		Integer seq = channelSeq.getAndIncrement();
		futureMap.put(seq, future);
		return seq;
	}
	
	public NkvFuture getResult(Integer seq) {
		return futureMap.get(seq);
	}
	
	public ChannelFuture sendPacket(ChannelBuffer buffer) throws NkvException {
		ChannelFuture future = channelImpl.write(buffer);
		return future;
	}
}
