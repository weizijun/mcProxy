package com.netease.backend.nkv.client.rpc.future;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.NkvBlockingQueue;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;
import com.netease.backend.nkv.client.rpc.net.NkvFuture.NkvFutureListener;

public class NkvResultFutureSetImpl<S extends AbstractResponsePacket, E, T extends ResultMap<byte[], Result<E>>> extends NkvResultFuture<ResultMap<byte[], Result<E>>>
{ 
	private Set<NkvResultFutureImpl<S, Result<T>>> futures;
	public NkvResultFutureSetImpl(Set<NkvResultFutureImpl<S, Result<T>>> futures) {
		this.futures = futures;
	}
	public void addFuture(NkvResultFutureImpl<S, Result<T>> future) {
		this.futures.add(future);
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean res = true;
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			res &= future.cancel(mayInterruptIfRunning);
		}
		return res;
	}

	public boolean isCancelled() {
		boolean res = true;
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			res &= future.isCancelled();
		}
		return res;
	}

	public boolean isDone() {
		boolean res = true;
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			res &= future.isDone();
		}
		return res;
	}

	public ResultMap<byte[], Result<E>> get() throws InterruptedException, ExecutionException {
		ResultMap<byte[], Result<E>> res = new ResultMap<byte[], Result<E>>();
		Set<ResultCode> codes = new HashSet<ResultCode>();
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			Result<T> r = future.get();
			codes.add(r.getCode());
			res.putAll(r.getResult());
		}
		res.setResultCode(codes);
		return res;
	}
	
	public ResultMap<byte[], Result<E>> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		ResultMap<byte[], Result<E>> res = new ResultMap<byte[], Result<E>>();
		long start = 0, end = 0;
		long consumeTime = timeout;
		Set<ResultCode> codes = new HashSet<ResultCode>();
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			Result<T> r = null;
			if (future.isDone()) {
				r = future.get(timeout, unit);
			}
			else {
				start = System.currentTimeMillis();
				r = future.get(consumeTime, unit);
				end = System.currentTimeMillis();
				consumeTime -= (end - start);
				if (consumeTime < 0) {
					throw new TimeoutException("get future timeout.");
				}
			}
			if (r != null) {
				codes.add(r.getCode());
				res.putAll(r.getResult());
			}
		}
		res.setResultCode(codes);
		return res;
	}

	public ResultMap<byte[], Result<E>> getPortion() throws InterruptedException, ExecutionException {
		ResultMap<byte[], Result<E>> res = new ResultMap<byte[], Result<E>>();
		 
		Set<ResultCode> codes = new HashSet<ResultCode>();
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			if (future.isDone()) {
				Result<T> r = future.get();
				codes.add(r.getCode());
				res.putAll(r.getResult());
			}
		}
		res.setResultCode(codes);
		return res;
	}

	public ResultMap<byte[], Result<E>> getPortion(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		ResultMap<byte[], Result<E>> res = new ResultMap<byte[], Result<E>>();

		long time = 0;
		long consumeTime = timeout;
		Set<ResultCode> codes = new HashSet<ResultCode>();
		for (NkvResultFutureImpl<S, Result<T>>  future : futures) {
			Result<T> r = null;
			if (future.isDone()) {
				r = future.get(timeout, unit);
			}
			else {
				time = System.currentTimeMillis();
				r = future.get(consumeTime, unit);
				consumeTime = consumeTime - (System.currentTimeMillis() - time);
				if (consumeTime <= 0) {
					break;
				}
			}
			if (r != null) {
				codes.add(r.getCode());
				res.putAll(r.getResult());
			}
		}
		res.setResultCode(codes);
		return res;
	}
	
	@Override
	public void futureNotify(final NkvBlockingQueue queue) {
		final AtomicInteger count = new AtomicInteger(futures.size());
		NkvFutureListener listener = new NkvFutureListener() {
			public void handle(Future<NkvRpcPacket> future) {
				if (count.decrementAndGet() == 0) {
					queue.offer(NkvResultFutureSetImpl.this);
				}
			}
		};
		for (NkvResultFutureImpl<S, Result<T>> future : futures) {
			future.setListener(listener);
		}
	}	
}