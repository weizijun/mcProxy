package com.netease.backend.nkv.client.rpc.net;

import java.net.SocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.ServerManager;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.impl.invalid.InvalidServer;
import com.netease.backend.nkv.client.packets.AbstractPacket;
import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.packets.dataserver.TrafficCheckResponse;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.client.rpc.protocol.tair2_3.PacketManager;

public class NkvRpcContext {
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private ReentrantReadWriteLock channelMapLock = new ReentrantReadWriteLock();

	private ConcurrentHashMap<SocketAddress, NkvChannel> channelMap = new ConcurrentHashMap<SocketAddress, NkvChannel>();

	private NkvConnector connector = null;
	private NkvBgWorker  bgWorker = null;
	private ServerManager serverManager = null;
	private String group = null;
	class FailCounter {
		
		protected ServerManager serverManager = null;
		protected int maxFailCount = 100;
	    protected AtomicInteger failCounter = new AtomicInteger(0);
	    public FailCounter(ServerManager serverManager) {
	    	this.serverManager = serverManager;
	    }
	    public FailCounter() {
	    }
	    public void setServerManager(ServerManager serverManager) {
	    	this.serverManager = serverManager;
	    }
	    public void setMaxFailCount(int maxFailedCount) {
	    	this.maxFailCount = maxFailedCount;
	    }
	    public int getMaxFailCount() {
	    	return this.maxFailCount;
	    }
	    public void hadFail() {
	    	if (failCounter.incrementAndGet() >= this.maxFailCount) {
	    		if (serverManager != null) {
	    			serverManager.checkVersion(1);
	    		}
	    		failCounter.set(0);
	    	}
	    }
	}
	private FailCounter failCounter = null;

	public NkvRpcContext(NioClientSocketChannelFactory nioFactory, NkvBgWorker bgWorker, String groupName) {
		connector = new NkvConnector(this, nioFactory);
		this.bgWorker = bgWorker;
		this.group = groupName;
		failCounter = new FailCounter();
	}
	public void setServerManager(ServerManager serverManager) {
		this.serverManager = serverManager;
		this.failCounter.setServerManager(serverManager);
		this.group = this.serverManager.getRowGroupName();
	}
	public void setMaxFailCount(int maxFailCount) {
		if (failCounter != null) {
			failCounter.setMaxFailCount(maxFailCount);
		}
	}
	public int getMaxFailCount() {
		if (failCounter != null) {
			return failCounter.getMaxFailCount();
		}
		return 0;
	}
	
	private void deleteSession(NkvChannel channel) {
		if (channelMap.get(channel.getDestAddress()) == channel) {
			// double check removed channel  
			NkvChannel deleteChannel = null;
			try {
				channelMapLock.writeLock().lock();
				if (channelMap.get(channel.getDestAddress()) == channel)
					deleteChannel = channelMap.remove(channel.getDestAddress());

			} finally {
				channelMapLock.writeLock().unlock();
			}
			if (deleteChannel != null)
				deleteChannel.close();
			log.info("GroupName: " + this.group + ", channel[id]: " + channel.getDestAddress() + "has be removed.");
		}
	}

	private NkvChannel obtainSession(SocketAddress addr, NkvRpcPacketFactory factory, long waittime) throws NkvRpcError {

		NkvChannel existChannel = null;
		channelMapLock.readLock().lock();
		existChannel = channelMap.get(addr);
		channelMapLock.readLock().unlock();
		
		if (existChannel == null) {
			try {
				channelMapLock.writeLock().lock();
				existChannel = channelMap.get(addr);
				if (existChannel == null) {
					NkvChannel channel = new NkvChannel(connector, addr, factory);
					existChannel = channelMap.putIfAbsent(addr, channel);
					if (existChannel == null) {
						log.info("connect addr " + addr);
						existChannel = channel;
						existChannel.connect();
					}
				}
			} finally {
				channelMapLock.writeLock().unlock();
			}
		}
		//fix bug, should delete the session
		if (existChannel.getCause() != null) {
			deleteSession(existChannel);
			throw new NkvRpcError("GroupName: " + this.group + ", get channel failed:" + addr, existChannel.getCause());
		}

		if (existChannel.isReady() || waittime == 0) {		
			return existChannel;
		} else {
			if (existChannel.waitConnect(waittime)) {
				if (existChannel.getCause() != null) {
					throw new NkvRpcError("GroupName: " + this.group + ", connect [ip]:" + addr, existChannel.getCause());
				} else if (existChannel.isReady()) {
					return existChannel;
				} else {
					// Never reach here
					log.error("GroupName: " + this.group + ", why reach here? target[ip]: "+ addr.toString());
				}
			} 
			log.info("GroupName: " + this.group + ", create tair connection success, target[ip]: "+ addr.toString());
			return null;
		}
	}

	private void deleteSession(Channel ch) {
		NkvChannel channel = NkvChannel.getNkvChannel(ch);
		if (channel != null) {
			deleteSession(channel);
		}
		else {	
			log.warn("GroupName: " + this.group + " delete session with: null channel, connection: " + ch.getRemoteAddress());
		}
	}

	/*
	 * return null, timeout
	 */
	//for ds
	public <PacketT> NkvFuture callAsync(final NkvChannel channel,
			PacketT request, long timeout, NkvRpcPacketFactory factory)
			throws NkvRpcError {

		// step 1, get session
		// final NkvChannel channel = obtainSession(addr, factory, 0);
		// if (channel == null)
		// throw new NkvRpcError("aync call failed, session not created");

		// step 2, send request
		// step 2.1, build PacketWrapper and register packet

		final NkvRpcPacket requestWrapper = factory.buildWithBody(channel.incAndGetChannelSeq(), request);

		bgWorker.addWaitingChannelSeq(channel, requestWrapper.getChannelSeq(), timeout);
		final NkvFuture tairFuture = channel.registCallTask(requestWrapper.getChannelSeq());
		if (tairFuture == null) {
			throw new NkvRpcError("duplicate channel id, GroupName: " + this.group + ", remote: " + channel.getDestAddress().toString());
		}
		//set `failCounter
		tairFuture.setFailCounter(failCounter);
		ChannelFutureListener listener = new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future) throws Exception {
				// step 2.1, send
				if (future != null) {
					channel.decAndGetWaitConnectCount();
					if (future.getCause() != null) {
						tairFuture.setException(future.getCause());
						bgWorker.removeWaitingChannelSeq(channel.getWaitingChannelSeq());
						channel.getAndRemoveCallTask(requestWrapper.getChannelSeq());
						return;
					}
					channel.waitConnect(0);
				}
				try {
					channel.sendPacket(requestWrapper, tairFuture);
				} catch (NkvException e) {
					serverManager.maybeForceCheckVersion();
					throw new NkvRpcError(e.getMessage() + ", remote: " + channel.getDestAddress().toString(), e.getCause());

				}
			}
		};

		 
		if (channel.isReady() == false) {
			ChannelFuture connectFuture = channel.getConnectFuture();
			if (connectFuture == null) {
				serverManager.maybeForceCheckVersion();
				throw new NkvRpcError("concurrent error, GroupName: " + this.group + ", remote: " + channel.getDestAddress().toString());
			}
			if (channel.getWaitConnectCount() > 128) {
				tairFuture.setConnectFuture(connectFuture);
			} else {
				channel.incAndGetWaitConnectCount();
				connectFuture.addListener(listener);
			}
		} else {
			try {
				listener.operationComplete(null);
			} catch (Exception e) {
				serverManager.maybeForceCheckVersion();
				throw new NkvRpcError(e.getMessage() + "GroupName: " + this.group + ", remote: " + channel.getDestAddress().toString(), e.getCause());
			}
		}

		return tairFuture;
	}

	public <PacketT extends AbstractRequestPacket, S extends AbstractResponsePacket, T> NkvResultFutureImpl<S, Result<T>> callAsync(
			SocketAddress addr, PacketT request, final long timeout,
			Class<S> retCls, NkvRpcPacketFactory factory,
			NkvResultCast<S, Result<T>> cast) throws NkvRpcError, NkvFlowLimit {
		final NkvChannel channel = obtainSession(addr, factory, 0);
		if (channel == null) {
			NkvRpcError e = new NkvRpcError("aync call failed, session not created, GroupName: " + this.group + ", Area: " + request.getNamespace() + ",  remote: " + addr.toString());
			recordExceptionToEagleeye(ResultCode.FAILED, request.getNamespace(), PacketManager.getPacketCode(request.getClass()), e, addr);
			throw e; 
		}

		checkLevelDown(channel, factory, request.getNamespace());
		
		if (channel.isTrafficDataOverflow(request.getNamespace())) {
			NkvFlowLimit e = new NkvFlowLimit("rpc overflow, GroupName: " + this.group + ", Area: " + request.getNamespace() + ", remote: " + channel.getDestAddress().toString());
			recordExceptionToEagleeye(ResultCode.RPC_OVERFLOW, request.getNamespace(), PacketManager.getPacketCode(request.getClass()), e, addr);
			throw e;
		}
		NkvFuture future = this.callAsync(channel, request, timeout, factory);
		return new NkvResultFutureImpl<S, Result<T>>(future, retCls, cast, request.getContext());
	}
	public <PacketT extends AbstractRequestPacket, S extends AbstractResponsePacket, T> NkvResultFutureImpl<S, Result<T>> callInvalServerAsync(
			InvalidServer ivs, PacketT request, final long timeout,
			Class<S> retCls, NkvRpcPacketFactory factory,
			NkvResultCast<S, Result<T>> cast) throws NkvRpcError, NkvFlowLimit {
		SocketAddress addr = ivs.getAddress();
		final NkvChannel channel = obtainSession(addr, factory, 0);
		if (channel == null) {
			NkvRpcError e = new NkvRpcError("aync call failed, session not created, GroupName: " + this.group + ", Area: " + request.getNamespace() + ",  remote: " + addr.toString());
			recordExceptionToEagleeye(ResultCode.FAILED, request.getNamespace(), PacketManager.getPacketCode(request.getClass()), e, addr);
			throw e; 
		}

		NkvFuture future = this.callAsync(channel, request, timeout, factory);
		return new NkvResultFutureImpl<S, Result<T>>(future, retCls, cast, request.getContext());
	}

	public <PacketT extends AbstractPacket, S extends AbstractResponsePacket, T> NkvResultFutureImpl<S, Result<T>> callAsyncUnlimit(
			SocketAddress addr, PacketT request, final long timeout,
			Class<S> retCls, NkvRpcPacketFactory factory,
			NkvResultCast<S, Result<T>> cast) throws NkvRpcError,
			NkvFlowLimit {
		final NkvChannel channel = obtainSession(addr, factory, 0);
		if (channel == null) {
			throw new NkvRpcError("aync call failed, session not created, GroupName: " + this.group + ", remote: " + addr.toString());
		}
		NkvFuture future = this.callAsync(channel, request, timeout, factory);
		return new NkvResultFutureImpl<S, Result<T>>(future, retCls, cast, request.getContext());
	}

	public void messageReceived(Channel channel, NkvRpcPacket packet) throws Exception {
		NkvChannel tairChannel = NkvChannel.getNkvChannel(channel);
		
		if (packet.getChannelSeq() == -1) {
			packet.decodeBody();
			if (packet.getBody() instanceof TrafficCheckResponse) {
				TrafficCheckResponse tcr = (TrafficCheckResponse) packet.getBody();
				tairChannel.limitLevelTouch(tcr.getNamespace(), tcr.getStatus());
				
			} else {
				log.warn("GroupName: " + this.group + ", from the remote: " + channel.getRemoteAddress().toString() + " with seq=-1 packet, class: " + packet.getBody().getClass());
			}

		} else {
			bgWorker.removeWaitingChannelSeq(tairChannel.getWaitingChannelSeq());
			//NkvFuture
			NkvFuture future = tairChannel.getAndRemoveCallTask(packet.getChannelSeq());
			if (future == null)
				return;

			if (packet.hasConfigVersion()) {
				serverManager.checkVersion(packet.decodeConfigVersion());
			}
			future.setValue(packet);
		}
		

	}

	public void exceptionCaught(Channel channel, Throwable cause)
		throws Exception {
		log.warn("Exception, will close connection", cause);
		deleteSession(channel);
	}

	public void channelDisconnected(Channel channel) {
		if (channel != null) {
			SocketAddress local = channel.getLocalAddress();
			SocketAddress remote = channel.getRemoteAddress();
			log.info("GroupName: " + this.group + ", LINK: [" + local.toString() + ", " + remote.toString() + " ] was disconnected by remote peer.");
			deleteSession(channel);
		}
	}

	protected void checkLevelDown(NkvChannel channel, NkvRpcPacketFactory factory, short ns) throws NkvRpcError {
		FlowLimit flowLimit = channel.getFlowLimitLevel(ns);
		if (flowLimit == null) {
			return ;
		} 
		flowLimit.limitLevelCheck(this, factory, channel, ns);
	}

	public void recordExceptionToEagleeye(ResultCode rc, short namespace,  int pcode, Exception e, SocketAddress addr) {
		//TODO
	}
}
