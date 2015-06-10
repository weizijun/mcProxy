package com.netease.backend.nkv.client;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvQueueOverflow;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.error.NkvTimeout;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureSetImpl;


public interface NkvClient {
	
	public static class NkvOption {
		private RequestOption requestOption;
		private long timeout;
		
		public NkvOption(long timeout, short version, int expire) {
			this.requestOption = new RequestOption(version, expire);
			this.timeout = timeout;
		}
		public NkvOption(long timeout, short version) {
			 this(timeout, version, 0);
		}
		public NkvOption(long timeout) {
			this(timeout, (short)0, 0);
		}
		public NkvOption() {
			this(Long.MAX_VALUE, (short)0, 0);
		}
		
		public RequestOption getRequestOption() {
			return this.requestOption;
		}
		public void setTimeout(long timeout) {
			this.timeout = timeout;
		}
		public void setVersion(short version) {
			this.requestOption.version = version;
		}
		public void setExpireTime(int expire) {
			this.requestOption.expire = expire;
		}
		public long getTimeout() {
			return timeout;
		}
		public short getVersion() {
			return requestOption.version;
		}
		public int getExpire() {
			return requestOption.expire;
		}
	}

	public static class RequestOption {
		private short version;
		private int expire;
		public void setVersion(short version) {
			this.version = version;
		}
		public void setExpire(int expire) {
			this.expire = expire;
		}
		public short getVersion() {
			return version;
		}
		public int getExpire() {
			return expire;
		}
		public RequestOption() {
			this.version = 0;
			this.expire = 0;
		}
		public RequestOption(short version, int expire) {
			this.version = version;
			this.expire = expire;
		}
	}

	public static class Counter {
		private int value = 0;
		private int initValue = 0;
		private int expire = 0;
		public Counter() {
			
		}
		public Counter(int value, int initValue, int expire) {
			this.value = value;
			this.initValue = initValue;
			this.expire = expire;
		}
		public int getValue() {
			return value;
		}
		public int getInitValue() {
			return initValue;
		}
		public int getExpire() {
			return expire;
		}
		public void setExpire(int expire) {
			this.expire = expire;
		}
		public void negated() {
			this.value = - this.value;
		}
	}

	public static class Pair<F, S> {
		protected F first = null;
		protected S second = null;
		public Pair(F first, S second) {
			this.first = first;
			this.second = second;
		}
		public F first() {
			return first;
		}
		public S second() {
			return second;
		}
		public boolean isAvaliable() {
			return first != null && second != null;
		}
	}
	
	public NkvResultFutureImpl<ReturnResponse, Result<Void>> putAsync(short ns, byte[] key, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public Future<Result<Void>> putIfNoExistAsync(short ns, byte[] key, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public NkvResultFutureImpl<GetResponse, Result<byte[]>> getAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit ;
	
public NkvResultFutureSetImpl<GetResponse, byte[], ResultMap<String, Result<byte[]>>> batchGetAsync(short ns, final List<byte[]> keys, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public Future<Result<Void>> setCountAsync(short ns, byte[] key, int count, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	 
	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	 
	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	 
	//public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	 
	//public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit;

	public Future<Result<Void>> lockAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	 
	public Future<Result<Void>> unlockAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	 
	public Future<Result<byte[]>> getHiddenAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit;

	 
	public Future<Result<Void>> prefixPutAsync(short ns, byte[] pkey, byte[] skey, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit;

	public Future<Result<byte[]>> prefixGetAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public Future<Result<Void>> prefixSetCountAsync(short ns, byte[] pkey, byte[] skey, int count, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public Future<Result<byte[]>> prefixGetHiddenAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int value, int initValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	//public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	//public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit;
	
	
	public Future<Result<Void>> hideByProxyAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException;
	
	public Future<Result<Void>> prefixHideByProxyAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException;
	
	public Future<Result<Void>> invalidByProxyAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException;
	
	public Future<Result<Void>> prefixInvalidByProxyAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException;

	public class NotifyFuture{
		private Future<?> future;
		private Object    ctx;
		public Future<?> getFuture() {
			return future;
		}
		public void setFuture(Future<?> future) {
			this.future = future;
		}
		public Object getCtx() {
			return ctx;
		}
		public void setCtx(Object ctx) {
			this.ctx = ctx;
		}
		public NotifyFuture(Future<?> future, Object ctx) {
			super();
			this.future = future;
			this.ctx = ctx;
		}
	}
	
	public void notifyFuture(Future<?> future, Object ctx) throws NkvQueueOverflow;
	
	public NotifyFuture poll(long timeout, TimeUnit unit) throws InterruptedException;
	
	public NotifyFuture poll() throws InterruptedException;
}
