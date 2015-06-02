package com.netease.backend.nkv.client.rpc.net;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
import com.netease.backend.nkv.client.rpc.net.NkvFuture.NkvFutureListener;



public class NkvCallFuture<T extends AbstractResponsePacket> implements Future<T> {
	
	NkvFuture impl;
	Class<T> retCls;
	
	protected T excutionException(Object response) {
		if (response instanceof ReturnResponse) {
			ReturnResponse r = (ReturnResponse) response;
			T t = null; 
			try {
				t = retCls.newInstance();
			} catch (InstantiationException e1) {
				e1.printStackTrace();
			} catch (IllegalAccessException e1) {
				e1.printStackTrace();
			}
			if (t != null) {
				t.setCode(r.getCode());
			}
			return t;
		}
		//never return null;
		return null;
	}
	public NkvCallFuture(NkvFuture impl, Class<T> retCls) {
		this.impl = impl;
		this.retCls = retCls;
	}
		
	public void setListener(NkvFutureListener listener) {
		impl.setListener(listener);
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		return impl.cancel(mayInterruptIfRunning);
	}

	public boolean isCancelled() {
		return impl.isCancelled();
	}

	public boolean isDone() {
		return impl.isDone();
	}

	public T get() throws InterruptedException, ExecutionException {
		NkvRpcPacket p = impl.get();
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			return retCls.cast(p.getBody());
		} catch (ClassCastException e) {
			return excutionException(p.getBody());
		}
	}

	public T get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException,
			TimeoutException {
		NkvRpcPacket p = impl.get(timeout, unit);
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			return retCls.cast(p.getBody());
		} catch (ClassCastException e) {
			return excutionException(p.getBody());
		}
	}
	
}