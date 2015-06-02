package com.netease.backend.nkv.client.rpc.future;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.netease.backend.nkv.client.NkvBlockingQueue;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.rpc.net.NkvFuture.NkvFutureListener;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;

public class NkvResultFutureListImpl<S extends AbstractResponsePacket, E> extends NkvResultFuture<Result<List<E>>>
{ 
	private Set<NkvResultFutureImpl<S, Result<List<E>>>> futures;
	public NkvResultFutureListImpl(Set<NkvResultFutureImpl<S, Result<List<E>>>> futures) {
		this.futures = futures;
	}
	public void addFuture(NkvResultFutureImpl<S, Result<List<E>>> future) {
		this.futures.add(future);
	}

	public boolean cancel(boolean mayInterruptIfRunning) {
		boolean res = true;
		for (NkvResultFutureImpl<S, Result<List<E>>> future : futures) {
			res &= future.cancel(mayInterruptIfRunning);
		}
		return res;
	}

	public boolean isCancelled() {
		boolean res = true;
		for (NkvResultFutureImpl<S, Result<List<E>>> future : futures) {
			res &= future.isCancelled();
		}
		return res;
	}

	public boolean isDone() {
		boolean res = true;
		for (NkvResultFutureImpl<S, Result<List<E>>> future : futures) {
			res &= future.isDone();
		}
		return res;
	}

	public Result<List<E>> get() throws InterruptedException, ExecutionException {
		Result<List<E>> res = new Result<List<E>>();
		List<E> lst = new ArrayList<E>();
		res.setResult(lst);
		for (NkvResultFutureImpl<S, Result<List<E>>> future : futures) {
			Result<List<E>> r = future.get();
			//fast fail
			if (r.getCode() != ResultCode.OK) {
				res.setCode(r.getCode());
				return res;
			}
			lst.addAll(r.getResult());
		}
		res.setCode(ResultCode.OK);
		return res;
	}
	
	public Result<List<E>> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		Result<List<E>> res = new Result<List<E>>();
		List<E> lst = new ArrayList<E>();
		res.setResult(lst);
		long start = 0, end = 0;
		long consumeTime = timeout;
		for (NkvResultFutureImpl<S, Result<List<E>>> future : futures) {
			Result<List<E>> r = null;
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
				if (r.getCode() != ResultCode.OK) {
					//fast fail
					res.setCode(r.getCode());
					return res;
				}
				
				if (r.getResult() != null) {
					lst.addAll(r.getResult());
				}
			}
		}
		res.setCode(ResultCode.OK);
		return res;
	}

	@Override
	public void futureNotify(final NkvBlockingQueue queue) {
		final AtomicInteger count = new AtomicInteger(futures.size());
		NkvFutureListener listener = new NkvFutureListener() {
			public void handle(Future<NkvRpcPacket> future) {
				if (count.decrementAndGet() == 0) {
					queue.offer(NkvResultFutureListImpl.this);
				}
			}
		};
		for (NkvResultFutureImpl<S, Result<List<E>>> future : futures) {
			future.setListener(listener);
		}
	}	
}