package com.netease.backend.nkv.client.rpc.future;

import java.util.concurrent.Future;

import com.netease.backend.nkv.client.NkvBlockingQueue;

public abstract class NkvResultFuture<T> implements Future<T> {
	Object ctx;
	
	public void setContext(Object c) {
		this.ctx = c; 
	}
	
	public Object getContext() {
		return ctx;
	}
	
	public abstract void futureNotify(final NkvBlockingQueue queue);
}