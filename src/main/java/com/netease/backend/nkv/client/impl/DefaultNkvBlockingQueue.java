package com.netease.backend.nkv.client.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.netease.backend.nkv.client.NkvBlockingQueue;
import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;

public class DefaultNkvBlockingQueue implements NkvBlockingQueue {
	protected BlockingQueue<NkvResultFuture<?>> queue = null;
	
	public DefaultNkvBlockingQueue() {
		queue = new LinkedBlockingQueue<NkvResultFuture<?>>();
	}
	public NkvResultFuture<?> poll() {
		return queue.poll();
	}
	
	public NkvResultFuture<?> poll(long timeout, TimeUnit unit) throws InterruptedException {
		return queue.poll(timeout, unit);
	}

	public boolean offer(NkvResultFuture<?> e) {
		return queue.offer(e);
	}

	public boolean offer(NkvResultFuture<?> e, long timeout, TimeUnit unit) throws InterruptedException {
		return queue.offer(e, timeout, unit);
	}
	
	public void clear() {
		queue.clear();
	}
	
	public int size() {
		return queue.size();
	}

	public boolean add(NkvResultFuture<?> e) {
		return queue.add(e);
	}
}
