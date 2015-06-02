package com.netease.backend.nkv.client;

import java.util.concurrent.TimeUnit;

import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;

public interface NkvBlockingQueue {
	public NkvResultFuture<?> poll();
	public NkvResultFuture<?> poll(long timeout, TimeUnit unit) throws InterruptedException;
	public boolean offer(NkvResultFuture<?> e);
	public boolean offer(NkvResultFuture<?> e, long timeout, TimeUnit unit) throws InterruptedException;
	public void clear();
	public int size();
}
