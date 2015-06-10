package com.netease.backend.nkv.client.rpc.net;

 
import java.net.SocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import com.netease.backend.nkv.client.error.NkvAgain;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.rpc.net.NkvRpcContext.FailCounter;
import com.netease.backend.nkv.mcProxy.QueryMsg;

public class NkvFuture implements java.util.concurrent.Future<NkvRpcPacket> {
	private ReentrantLock lock = new ReentrantLock();
	private Condition cond = lock.newCondition();
	private NkvRpcPacket packet = null;
	private Throwable exception = null;
	private SocketAddress addr = null;
	private ChannelFuture connectFuture; 
	private FailCounter failCounter = null;
	private int waitCount = 0;
	
	NkvFutureListener listener = null;
	
	private QueryMsg queryMsg;
	
	public interface NkvFutureListener {
		public void handle(Future<NkvRpcPacket> future);
	}
	 
	public void setRemoteAddress(SocketAddress addr) {
		this.addr = addr;
	}
	public SocketAddress getRemoteAddress() {
		return this.addr;
	}
	public void setFailCounter(FailCounter failCounter) {
		this.failCounter = failCounter;
	}
	
	public void setConnectFuture(ChannelFuture connectFuture) {
		this.connectFuture = connectFuture;
		this.connectFuture.addListener(new ChannelFutureListener() {
			public void operationComplete(ChannelFuture future)
					throws Exception {
				innerNotifyAll();
			}
		});
	}
	
	public void setValue(NkvRpcPacket p) throws NkvException {
		this.packet = p; 
		innerNotifyAll();
	}
	
	public boolean setException(Throwable r) {
		this.exception = r;
		if (failCounter != null) {
			failCounter.hadFail();
		} 
		innerNotifyAll();
		return false;
	}
	
	public void setListener(NkvFutureListener listener) {
		NkvFutureListener nowCall = null;
		try {
			lock.lock();
			if (isDone()) {
				nowCall = listener;
			} else {
				this.listener = listener;
			}
		} finally {
			lock.unlock();
		}
		if (nowCall != null) {
			nowCall.handle(this);
		}
	}
	
	private void innerNotifyAll() {
		NkvFutureListener nowCall = null;
		try {
			lock.lock();
			if (waitCount <= 1) {
				cond.signal();
			} else if (waitCount > 1) {
				cond.signalAll();
			}
			if (this.listener != null) {
				nowCall = this.listener;
				this.listener = null;
			}
		} finally {
			lock.unlock();
		}
		if (nowCall != null) {
			nowCall.handle(this);
		}
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		throw new UnsupportedOperationException();
	}

	public boolean isCancelled() {
		throw new UnsupportedOperationException();
	}

	public boolean isDone() {
		boolean connectDone = false;
		if (connectFuture != null)
			connectDone = connectFuture.isDone();
		
		return connectDone || (packet != null || exception != null);
	}
	
	private NkvRpcPacket innerGet() throws ExecutionException {
		if (packet != null) {
			try {
				packet.decodeBody();
			} catch (NkvException e) {
				String message = e.getMessage() + " remote: " + this.addr;
				throw new ExecutionException(message, e.getCause());
			}
			return packet;
		}
		
		if (exception != null) {
			throw new ExecutionException("remote: " + this.addr, exception);
		}
		
		if (connectFuture != null) {		
			if (connectFuture.getCause() != null)
				throw new ExecutionException("remote: " + this.addr, connectFuture.getCause());
			throw new ExecutionException("remote: " + this.addr, new NkvAgain());
		}
		throw new ExecutionException(new Exception("no result had been set, remote: " + this.addr));
	}
	
	private void innerWait(long timeout, TimeUnit unit) throws InterruptedException {
		try {
			lock.lock();
			waitCount++;
			if (!isDone()) {
				if (timeout == -1)
					cond.await();
				else
					cond.await(timeout, unit);
			}
		} finally {
			waitCount--;
			lock.unlock();
		}
	}

	public NkvRpcPacket get() throws InterruptedException, ExecutionException {
		if (!isDone()) {
			innerWait(-1, null);
		}
		return innerGet();
	}

	public NkvRpcPacket get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		if (!isDone()) {
			innerWait(timeout, unit);
		}
		if (!isDone()) {
			throw new TimeoutException("Timeout, remote: " + this.addr);
		}
		return innerGet();
	}
	public QueryMsg getQueryMsg() {
		return queryMsg;
	}
	public void setQueryMsg(QueryMsg queryMsg) {
		this.queryMsg = queryMsg;
	}
}
