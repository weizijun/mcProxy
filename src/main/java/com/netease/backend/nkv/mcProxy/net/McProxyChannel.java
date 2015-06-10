package com.netease.backend.nkv.mcProxy.net;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;

import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;
import com.netease.backend.nkv.mcProxy.QueryMsg;
import com.netease.backend.nkv.mcProxy.command.Command;

/**
 * @author hzweizijun 
 * @date 2015年6月2日 下午4:15:56
 */
public class McProxyChannel {
	private Channel channelImpl = null;
	private final static AtomicInteger channelSeq ;
	private ConcurrentMap<Integer, NkvFuture> futureMap = new ConcurrentHashMap<Integer, NkvFuture>();
	private ConcurrentMap<Integer, NkvResultFuture<?>> futureResultMap = new ConcurrentHashMap<Integer, NkvResultFuture<?>>();
	private BlockingQueue<QueryMsg> msgQueue = new LinkedBlockingQueue<QueryMsg>();
	private ReentrantLock headLock = new ReentrantLock();
	private Command cacheParsingCommand;
	
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
	
	public Integer putResultFuture(NkvResultFuture<?> future) {
		Integer seq = channelSeq.getAndIncrement();
		futureResultMap.put(seq, future);
		return seq;
	}
	
	public NkvResultFuture<?> getResultFuture(Integer seq) {
		return futureResultMap.get(seq);
	}
	
	public NkvFuture getResult(Integer seq) {
		return futureMap.get(seq);
	}
	
	public ChannelFuture sendPacket(ChannelBuffer buffer) throws NkvException {
		ChannelFuture future = channelImpl.write(buffer);
		return future;
	}
	
	public void offerMsg(QueryMsg msg) {
		msgQueue.offer(msg);
	}
	
	public QueryMsg getFirstMsg() {
		return msgQueue.peek();
	}
	
	public QueryMsg pollFirstMsg() {
		return msgQueue.poll();
	}
	
	public void lockHead() {
		headLock.lock();
	}
	
	public void unlockHead() {
		headLock.unlock();
	}

	public Command getCacheParsingCommand() {
		return cacheParsingCommand;
	}

	public void setCacheParsingCommand(Command cacheParsingCommand) {
		this.cacheParsingCommand = cacheParsingCommand;
	}
}
