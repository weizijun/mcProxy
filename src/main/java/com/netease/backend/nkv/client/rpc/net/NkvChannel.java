 	package com.netease.backend.nkv.client.rpc.net;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvTimeout;
import com.netease.backend.nkv.client.rpc.net.FlowLimit.FlowStatus;


public class NkvChannel {
	protected static final Logger log = LoggerFactory.getLogger(NkvChannel.class);

	private NkvConnector connector;
	private Channel channelImpl = null;
	private Throwable cause = null;

	private Object sessionLock = new Object();
	private Boolean finished = false;
	private SocketAddress destAddress;
	private NkvRpcPacketFactory factory;
	private final static AtomicInteger channelSeq ;
	
	private ChannelFuture connectFuture;
	
	private AtomicInteger waitConnectCount = new AtomicInteger(0);
	
	private ConcurrentHashMap<Short, FlowLimit> flowLimitLevel = new ConcurrentHashMap<Short, FlowLimit>();
	
	static {
		channelSeq = new AtomicInteger(1);
	}
	
	private ConcurrentHashMap<Integer, NkvFuture> tasks = new ConcurrentHashMap<Integer, NkvFuture>();
	
	private NkvRpcPacket cachedPacketWrapper;
	
	private WaitingChannelSeq waitingChannelSeq;
	
	public NkvRpcPacket getCachedPacketWrapper() {
		return cachedPacketWrapper;
	}
	
	public void SetWaitingChannelSeq(WaitingChannelSeq waitingChannelSeq) {
		this.waitingChannelSeq = waitingChannelSeq;
	}
	
	public WaitingChannelSeq getWaitingChannelSeq() {
		return this.waitingChannelSeq;
	}

	public FlowLimit getFlowLimitLevel(short ns) {
		return flowLimitLevel.get(ns);
	}
	
	public int getCurrentThreshold(short ns) {
		FlowLimit fl = flowLimitLevel.get(ns);
		if (fl != null) {
			return fl.getThreshold();
		}
		return 0;
	}
	
	public void setCachedPacketWrapper(NkvRpcPacket cachedPacketWrapper) {
		this.cachedPacketWrapper = cachedPacketWrapper;
	}
	
	public int incAndGetWaitConnectCount() {
		return waitConnectCount.getAndIncrement();
	}
	
	public int decAndGetWaitConnectCount() {
		return waitConnectCount.decrementAndGet();
	}
	
	public int getWaitConnectCount() {
		return waitConnectCount.get();
	}
	
	public int incAndGetChannelSeq() {
		int chid = 0;//channelSeq.getAndIncrement();
		do {
			chid = channelSeq.getAndIncrement();
		} while (chid == -1 || chid == 0);
		return chid;
	}

	public static NkvChannel getNkvChannel(Channel ctx)	 {
		return (NkvChannel)ctx.getAttachment();
	}
	
	public NkvRpcPacketFactory getPacketFactory() {
		return factory;
	}
	
	ChannelFutureListener ioFutureListener = new ChannelFutureListener() {
		
		public void operationComplete(ChannelFuture future) throws Exception {
			cause = future.getCause();
			
			channelImpl = future.getChannel();
			channelImpl.setAttachment(NkvChannel.this);
			
			synchronized (sessionLock) {
				finished = true;
				sessionLock.notifyAll();
			}
		}
	};
	
	public NkvChannel(NkvConnector connector, SocketAddress destAddress, NkvRpcPacketFactory factory) {
		this.connector 		= connector;
		this.destAddress	= destAddress;
		this.factory		= factory;
	}
	
	public ChannelFuture connect() {
		return connectFuture = connector.createSession(destAddress, ioFutureListener);
	}
	
	public ChannelFuture getConnectFuture() {
		return connectFuture;
	}
	
	public SocketAddress getDestAddress() {
		return destAddress;
	}
	
	public boolean isReady() {
		return channelImpl != null && cause == null;
	}
	
	public Throwable getCause() {
		return cause;
	}
	
	public boolean isTrafficDataOverflow(short ns) {
		if (ns <= 0) return false;
		
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null)
			return false;
		boolean ret = flowLimit.isOverflow();
		if (ret)
			log.debug("overflow threshold: " + flowLimit.getThreshold());
		return ret;
	}
	
	public boolean limitLevelUp(short ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			flowLimit = new FlowLimit(ns);
			flowLimitLevel.putIfAbsent(ns, flowLimit);
			flowLimit = flowLimitLevel.get(ns);
		} 
		boolean ret = flowLimit.limitLevelUp();
		log.warn("overflow threshold up: " + flowLimit.getThreshold());
		return ret;
	}
	
	public boolean limitLevelDown(short ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return false;
		} 
		boolean ret = flowLimit.limitLevelDown();
	    log.warn("oveflow threshold down: " + flowLimit.getThreshold());
		return ret;
	}
	
	public void limitLevelTouch(short ns, FlowStatus status) {
		 
		switch (status) {
		case KEEP:
			limitLevelTouch(ns);
			break;
		case UP:
			limitLevelUp(ns);
			break;
		case DOWN:
			limitLevelDown(ns);
			break;
		default:
			break;
		}
	}
		
	public void limitLevelTouch(short ns) {
		FlowLimit flowLimit = flowLimitLevel.get(ns);
		if (flowLimit == null) {
			return ;
		} 
		flowLimit.limitLevelTouch();
	}
	
	public ChannelFuture sendPacket(final NkvRpcPacket packet, final NkvFuture rpcFuture) throws NkvException {
		ChannelFuture future = channelImpl.write(packet.encode());
		if (rpcFuture != null) {
			future.addListener(new ChannelFutureListener() {
				public void operationComplete(ChannelFuture future)
						throws Exception {
					if (future.getCause() != null) {
						log.warn("send reuqest failed, cause:", future.getCause());
						rpcFuture.setException(future.getCause());
					} 
				}
			});
		}
		return future;
	}
	
	public <T> NkvFuture registCallTask(int channelSeq) {
		NkvFuture future = new NkvFuture();
		future.setRemoteAddress(destAddress);
		tasks.put(channelSeq, future);
		return future;
	}
	
	public NkvFuture getAndRemoveCallTask(int channelSeq) {
		NkvFuture future = tasks.remove(channelSeq);
		return future;
	}
	
	public NkvFuture clearTimeoutCallTask(int channelSeq) {
		NkvFuture future = tasks.remove(channelSeq);
		if (future != null) {
			future.setException(new NkvTimeout("waiting response timeout, remote: " + future.getRemoteAddress().toString()));
		}
		return future;
	}
	
	public boolean waitConnect(long waittime) {
		if (finished)
			return true;
		synchronized (sessionLock) {
			try {
				if (finished == false) {
					if (waittime == 0) {
						sessionLock.wait();
					} else {
						sessionLock.wait(waittime);
					}
				}
			} catch (InterruptedException e) {
				return false;
			}
			return finished;
		}
	}

	public void close() {
		if (channelImpl != null) {
			 try {
				channelImpl.close();
			 } catch (Exception e) {
			 	log.warn("close channel exception " + channelImpl, e);
			 }
		}
	}
}

