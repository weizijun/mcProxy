package com.netease.backend.nkv.client.impl;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.StreamResult;
import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.error.NkvTimeout;
import com.netease.backend.nkv.client.util.NkvConstant;

public class DefaultNkvClient extends AbstractNkvClient {
	protected static final Logger log = LoggerFactory.getLogger(DefaultNkvClient.class);
	
	public StreamResult<byte[]> exportKeys(short ns, NkvOption option) 
			throws NkvRpcError, NkvFlowLimit, InterruptedException, ExecutionException, TimeoutException {
		List<SocketAddress> dsList = tairProcessor.getDsList();
		Map<SocketAddress, Integer> offsetMap = new HashMap<SocketAddress, Integer>(dsList.size());
		for (SocketAddress addr : dsList) {
			offsetMap.put(addr, 0);
		}
		long timeout = option == null ? defaultOptions.getTimeout() : option.getTimeout();
		Future<Result<List<byte[]>>> first = getBatchKeys(ns, offsetMap, exportBatchNum, timeout);
		Result<List<byte[]>> firstResult = first.get(timeout, TimeUnit.MILLISECONDS);
		return new KeyDump(ns, this, firstResult, offsetMap, timeout);
	}
	
	public StreamResult<Entry<byte[], byte[]>> exportKVs(short ns, NkvOption option) 
			throws NkvRpcError, NkvFlowLimit, InterruptedException, ExecutionException, TimeoutException {
		List<SocketAddress> dsList = tairProcessor.getDsList();
		Map<SocketAddress, Integer> offsetMap = new HashMap<SocketAddress, Integer>(dsList.size());
		for (SocketAddress addr : dsList) {
			offsetMap.put(addr, 0);
		}
		long timeout = option == null ? defaultOptions.getTimeout() : option.getTimeout();
		Future<Result<List<Entry<byte[], byte[]>>>> first = getBatchKVs(ns, offsetMap, exportBatchNum, timeout);
		Result<List<Entry<byte[], byte[]>>> firstResult = first.get(timeout, TimeUnit.MILLISECONDS);
		return new KVDump(ns, this, firstResult, offsetMap, timeout);
	}
	
	public Result<byte[]> get(short ns, byte[] key, NkvOption opt) 
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<byte[]>> future = getAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<byte[]> getHidden(short ns, byte[] key, NkvOption opt) 
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<byte[]>> future = getHiddenAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> put(short ns, byte[] key, byte[] value, NkvOption opt)
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = putAsync(ns, key, value, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> putIfNoExist(short ns, byte[] key, byte[] value, NkvOption opt)
		throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = putIfNoExistAsync(ns, key, value, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Integer> incr(short ns, byte[] key, int value, int defaultValue, NkvOption opt)
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		if (value < 0) {
			throw new IllegalArgumentException(NkvConstant.ITEM_VALUE_NOT_AVAILABLE);
		}
		Future<Result<Integer>> future = incrAsync(ns, key, value, defaultValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
//	public Result<Integer> incr(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt)
//			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
//		Future<Result<Integer>> future = incrAsync(ns, key, value, defaultValue, lowBound, upperBound, opt);
//		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
//	}

	public Result<Integer> decr(short ns, byte[] key, int value, int defaultValue, NkvOption opt)
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		if (value < 0) {
			throw new IllegalArgumentException(NkvConstant.ITEM_VALUE_NOT_AVAILABLE);
		}
		Future<Result<Integer>> future = decrAsync(ns, key, value, defaultValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

//	public Result<Integer> decr(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt)
//			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
//		Future<Result<Integer>> future = decrAsync(ns, key, value, defaultValue, lowBound, upperBound, opt);
//		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
//	}

	public Result<Void> lock(short ns, byte[] key, NkvOption opt)
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = lockAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> unlock(short ns, byte[] key, NkvOption opt)
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = unlockAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> prefixPut(short ns, byte[] pkey, byte[] skey, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = prefixPutAsync(ns, pkey, skey, value, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> prefixPutIfNoExist(short ns, byte[] pkey, byte[] skey, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = prefixPutIfNoExistAsync(ns, pkey, skey, value, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<byte[]> prefixGet(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<byte[]>> future = prefixGetAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<byte[]> prefixGetHidden(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<byte[]>> future = prefixGetHiddenAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	
	public Result<Void> setCount(short ns, byte[] key, int count, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = this.setCountAsync(ns, key, count, opt);
		return futureGet(future,opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}

	public Result<Void> prefixSetCount(short ns, byte[] pkey, byte[] skey, int count, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = this.prefixSetCountAsync(ns, pkey, skey, count, opt);
		return futureGet(future,opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}


	public Result<Integer> prefixIncr(short ns, byte[] pkey, byte[] skey, int value, int initValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Integer>> future = prefixIncrAsync(ns, pkey, skey, value, initValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
//	public Result<Integer> prefixIncr(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
//		Future<Result<Integer>> future = prefixIncrAsync(ns, pkey, skey, value, initValue, lowBound, upperBound, opt);
//		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
//	}
	
	public Result<Integer> prefixDecr(short ns, byte[] pkey, byte[] skey, int value, int initValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Integer>> future = prefixDecrAsync(ns, pkey, skey, value, initValue, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
//	public Result<Integer> prefixDecr(short ns, byte[] pkey, byte[] skey, int value, int initValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
//		Future<Result<Integer>> future = prefixDecrAsync(ns, pkey, skey, value, initValue, lowBound, upperBound, opt);
//		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
//	}

	public Result<Void> invalidByProxy(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = invalidByProxyAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	public Result<Void> hideByProxy(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = hideByProxyAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
	public Result<Void> prefixInvalidByProxy(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = prefixInvalidByProxyAsync(ns, pkey, skey, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}
		

	/*public Result<List<Pair<byte[], Result<byte[]>>>> getRange(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws InterruptedException, NkvTimeout, NkvRpcError, NkvFlowLimit {
		Future<Result<List<Pair<byte[], Result<byte[]>>>>> futureSet = getRangeAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}
	
	public Result<List<Result<byte[]>>> deleteRange(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws InterruptedException, NkvTimeout, NkvRpcError, NkvFlowLimit {
		Future<Result<List<Result<byte[]>>>> future = deleteRangeAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());		
	}


	public Result<List<Result<byte[]>>> getRangeKey(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws InterruptedException, NkvTimeout, NkvRpcError, NkvFlowLimit {
		Future<Result<List<Result<byte[]>>>> future = getRangeKeyAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	public Result<List<Result<byte[]>>> getRangeValue(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws InterruptedException, NkvTimeout, NkvRpcError, NkvFlowLimit {
		Future<Result<List<Result<byte[]>>>> future = getRangeValueAsync(ns, pkey, begin, end, offset, maxCount, reverse, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}*/

	/*public Result<Void> expire(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = expireAsync(ns, key, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}*/

	/*public ResultMap<byte[], Result<byte[]>> simplePrefixGetMulti(
			short ns, byte[] pkey, List<byte[]> skeys, NkvOption opt)
			throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<ResultMap<byte[], Result<byte[]>>> futureSet = simplePrefixGetMultiAsync(ns, pkey, skeys, opt);
		return futureGet(futureSet, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());
	}*/
	
	
	public Result<Map<String, String>> getStat(int qtype,
			String group, long serverId, NkvOption opt) throws NkvRpcError,
			NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Map<String, String>>> future = getStatAsync(qtype, group, serverId, opt);
		return futureGet(future, opt != null ? opt.getTimeout() : defaultOptions.getTimeout());	
	}

	private <T> T futureGet(Future<T> future, long timeout) throws InterruptedException, NkvTimeout, NkvRpcError {
		try {
			return future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			log.debug("exception: ", e);
			Throwable t = e.getCause();
			if (t instanceof NkvTimeout) {
				throw (NkvTimeout)t;
			} 
			if (t instanceof NkvRpcError) {
				throw (NkvRpcError)t;
			}
			throw new NkvRpcError(t);
		} catch (TimeoutException e) {
			throw new NkvTimeout(e);
		}
	}
}
