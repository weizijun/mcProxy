package com.netease.backend.nkv.client.rpc.future;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netease.backend.nkv.client.NkvBlockingQueue;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;
import com.netease.backend.nkv.client.rpc.net.NkvFuture.NkvFutureListener;

//SΪresponse packet��TΪ�����Ľ��������NkvResultCast��Ҫ�����ǽ�response packet�����ֵ��Result<T>
public class NkvResultFutureImpl<S extends AbstractResponsePacket, T> extends NkvResultFuture<T> {

	NkvFuture impl;//ΪNkvFuture
	NkvResultCast<S, T> cast;//���ڽ�response packetת��ΪResult<T>
	
	Class<S> retClst;//����ǿ������ת������packet��bodyǿ��ת��ΪS(AbstractResponsePacket)�������࣬��ʵpacket��body����retClst���͵�
	private Object context = null;
	
	public NkvResultFutureImpl(NkvFuture impl, Class<S> retCls, 
							NkvResultCast<S, T> cast, 
							Object context) {
		this.impl = impl;
		this.retClst = retCls;
		this.cast = cast;
		this.context = context;
		
	}

	class NkvFutureListenerImpl implements NkvFutureListener {
		final NkvResultFuture<T> inst;
		final NkvBlockingQueue queue;
		
		NkvFutureListenerImpl(NkvResultFuture<T> inst, NkvBlockingQueue queue) {
			this.inst = inst;
			this.queue = queue;
		}

		public void handle(Future<NkvRpcPacket> future) {
			queue.offer(inst);	
		}
	}

	public void futureNotify(final NkvBlockingQueue queue) {
		setListener(new NkvFutureListenerImpl(this, queue));
	}
	
	public void setListener(final NkvFutureListener listener) {
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
		return innerGet();
	}
	
	public S getResponse() throws InterruptedException, ExecutionException {
		return innerGetResponse();
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException {
		return innerGet(timeout, unit);
	}
	
	protected S excutionException(Object response) {
		if (response instanceof ReturnResponse) {
			ReturnResponse r = (ReturnResponse) response;
			S t = null; 
			try {
				t = retClst.newInstance();
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
	
	protected S innerGetResponse() throws InterruptedException, ExecutionException {
		S retPacket = null;
		NkvRpcPacket p = impl.get();
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			retPacket = retClst.cast(p.getBody());
		} catch (ClassCastException e) {
			retPacket = excutionException(p.getBody());
		}
		return retPacket;
	}
	
	protected T innerGet() throws InterruptedException, ExecutionException {
		S retPacket = null;
		NkvRpcPacket p = impl.get();
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			retPacket = retClst.cast(p.getBody());
		} catch (ClassCastException e) {
			retPacket = excutionException(p.getBody());
		}
		
		if (retPacket == null)
			return null;
		try {
			return cast.cast(retPacket, context);
		} catch (NkvRpcError e) {
			throw new ExecutionException(e);
		}
		catch (NkvCastIllegalContext e) {
			throw new ExecutionException(e);
		}
	}

	protected T innerGet(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException,
			TimeoutException {
		S retPacket = null;
		NkvRpcPacket p = impl.get(timeout, unit);
		if (p == null)
			throw new ExecutionException(new NullPointerException("futre<PacketWrapper> shouldn't return null"));
		try {
			retPacket =  retClst.cast(p.getBody());
		} catch (ClassCastException e) {
			retPacket = excutionException(p.getBody());
		}
		
		if (retPacket == null)
			return null;
		try {
			return cast.cast(retPacket, context);
		} catch (NkvRpcError e) {
			throw new ExecutionException(e);
		}
		catch (NkvCastIllegalContext e) {
			throw new ExecutionException(e);
		}
	}
}
