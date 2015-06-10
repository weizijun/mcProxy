package com.netease.backend.nkv.client.impl;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netease.backend.nkv.client.NkvBlockingQueue;
import com.netease.backend.nkv.client.NkvClient;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvQueueOverflow;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.error.NkvTimeout;
import com.netease.backend.nkv.client.impl.cast.NkvResultCastFactory;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
import com.netease.backend.nkv.client.packets.configserver.QueryInfoRequest;
import com.netease.backend.nkv.client.packets.configserver.QueryInfoResponse;
//import com.netease.backend.nkv.client.packets.dataserver.BoundedIncDecRequest;
//import com.netease.backend.nkv.client.packets.dataserver.BoundedPrefixIncDecRequest;
import com.netease.backend.nkv.client.packets.dataserver.DeleteRequest;
import com.netease.backend.nkv.client.packets.dataserver.DumpAllRequest;
import com.netease.backend.nkv.client.packets.dataserver.DumpAllResponse;
import com.netease.backend.nkv.client.packets.dataserver.DumpKeyRequest;
import com.netease.backend.nkv.client.packets.dataserver.DumpKeyResponse;
//import com.netease.backend.nkv.client.packets.dataserver.ExpireRequest;
import com.netease.backend.nkv.client.packets.dataserver.GetHiddenRequest;
import com.netease.backend.nkv.client.packets.dataserver.GetRequest;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.packets.dataserver.HideRequest;
import com.netease.backend.nkv.client.packets.dataserver.IncDecRequest;
import com.netease.backend.nkv.client.packets.dataserver.IncDecResponse;
import com.netease.backend.nkv.client.packets.dataserver.LockRequest;
import com.netease.backend.nkv.client.packets.dataserver.PutIfNoExistRequest;
import com.netease.backend.nkv.client.packets.dataserver.PutRequest;
import com.netease.backend.nkv.client.packets.dataserver.TouchRequest;
import com.netease.backend.nkv.client.packets.invalidserver.HideByProxyRequest;
import com.netease.backend.nkv.client.packets.invalidserver.InvalidByProxyRequest;
import com.netease.backend.nkv.client.rpc.future.NkvResultFuture;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureListImpl;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureSetImpl;
import com.netease.backend.nkv.client.rpc.net.DeamondThreadFactory;
import com.netease.backend.nkv.client.util.NkvConstant;
import com.netease.backend.nkv.client.util.NkvUtil;
//import com.netease.backend.nkv.client.packets.dataserver.RangeRequest;
//import com.netease.backend.nkv.client.packets.dataserver.RangeResponse;
//import com.netease.backend.nkv.client.packets.dataserver.SimplePrefixGetMultiRequest;
//import com.netease.backend.nkv.client.packets.dataserver.SimplePrefixGetMultiResponse;


public abstract class AbstractNkvClient implements NkvClient {
	private static Logger logger = LoggerFactory.getLogger(AbstractNkvClient.class);
	public static final NkvOption DEFAULT_OP = new NkvOption(500);
	public static final int EXPORT_BATCH_LIMIT = 200;
	protected static int exportBatchNum = EXPORT_BATCH_LIMIT;
	protected NkvOption defaultOptions = DEFAULT_OP;
	protected NkvProcessor tairProcessor = null;
	private String master;
	private String slave;
	private String group;
	private CompressOption compressOpt = new CompressOption();;
	
	private static int workerThreadCount = Runtime.getRuntime().availableProcessors() / 4 + 1;
	private static int bossThreadCount = (Runtime.getRuntime().availableProcessors() + 7) / 8;
	private static String workerThreadCountKey = "tair.nio.workercount";
    private static ExecutorService bossThreadPool = null;
    private static ExecutorService workerThreadPool = null;
    private static void initThreadCount() {
    	String workerThreadCountStr = System.getProperty(workerThreadCountKey);
    	if (workerThreadCountStr != null) {
    		try {
    			workerThreadCount = Integer.parseInt(workerThreadCountStr);
    			logger.info("worker thread from the system property: " + workerThreadCountStr);	 
    		} catch (NumberFormatException e) {
    			logger.error("failed to get the worker thread from the system property: " + workerThreadCountStr, e);	 
    		}
    	}
    }
    static {
    	initThreadCount();
    	bossThreadPool = Executors.newCachedThreadPool(new DeamondThreadFactory("nkv-boss-share"));
    	workerThreadPool = Executors.newCachedThreadPool(new DeamondThreadFactory("nkv-worker-share"));
    }
	
	private static NioClientSocketChannelFactory defaultNioFactory = new NioClientSocketChannelFactory(bossThreadPool, workerThreadPool, bossThreadCount, workerThreadCount);
	private NioClientSocketChannelFactory nioFactory = defaultNioFactory;
	
	
	private int maxNotifyQueueSize = 512;
	
	private static NkvBlockingQueue notifyQueue = new DefaultNkvBlockingQueue(); 
	
	public static int getExportBatchNum() {
		return exportBatchNum;
	}
	
	public static void setBatchExportNum(int num) {
		if (num <= 0 || num > EXPORT_BATCH_LIMIT)
			throw new IllegalArgumentException("batch export num must between 0 and " + EXPORT_BATCH_LIMIT);
		exportBatchNum = num;
	}
	public AbstractNkvClient() {
		defaultOptions.setVersion ((short)0);
		defaultOptions.setExpireTime(0);
		defaultOptions.setTimeout(500);
	}
	
	public void setTimeout(int timeout) {
		defaultOptions.setTimeout(timeout);
	}
	
	public void setMaxNotifyQueueSize(int maxNotifyQueueSize) {
		this.maxNotifyQueueSize = maxNotifyQueueSize;
	}
	
	public NkvBlockingQueue getNotifyQueue() {
		return notifyQueue;
	}

	public void setNotifyQueue(NkvBlockingQueue notifyQueueNew) {
		notifyQueue.clear();
		notifyQueue = notifyQueueNew;
	}

	public int getMaxNotifyQueueSize() {
		return maxNotifyQueueSize;
	}

	public void setMaster(String master) {
		this.master = master;
	}
	public String getMaster() {
		return master;
	}

	public void setSlave(String slave) {
		this.slave = slave;
	}
	public String getSlave() {
		return slave;
	}

	public void setWorkerThreadCount(int count) {
		workerThreadCount = count;
	}
	public int getWorkerThreadCount() {
		return workerThreadCount;
	}

	public static void setBossThreadCount(int count) {
		bossThreadCount = count;
	}
	
	public int getBossThreadCount() {
		return bossThreadCount;
	}

	public void setGroup(String group) {
		//if (group.endsWith("\0"))
			this.group = group;
		//else
		//	this.group = group + "\0";
	}
	
	public String getGroup() {
		return this.group;
	}

	public NioClientSocketChannelFactory getNioFactory() {
		return nioFactory;
	}

	public void setNioFactory(NioClientSocketChannelFactory nioFactory) {
		this.nioFactory = nioFactory;
	}
	
	public void setNkvProcessor(NkvProcessor tairProcessor) {
		this.tairProcessor = tairProcessor;
	}
	
	public boolean isCompressEnabled() {
		return compressOpt.isCompressEnabled();
	}

	public void setCompressEnabled(boolean compressEnabled) {
		compressOpt.compressEnabled = compressEnabled;
	}

	public boolean isUseFastCompress() {
		return compressOpt.useFastCompress;
	}

	public void setUseFastCompressed(boolean useFastCompress) {
		compressOpt.useFastCompress = useFastCompress;
	}

	public int getCompressThreshold() {
		return compressOpt.compressThreshold;
	}

	public void setCompressThreshold(int compressThreshold) {
		if (compressThreshold > NkvConstant.MAX_VALUE_SIZE)
			throw new IllegalArgumentException("compressThreshold must < NkvConstant.MAX_VALUE_SIZE which is " + 
					NkvConstant.MAX_VALUE_SIZE);
		compressOpt.compressThreshold = compressThreshold;
	}
	
	public void init() throws NkvException {
		//if (tairProcessor != null)
	    //		throw new NkvException("had inited");
		//tairProcessor = new NkvProcessor(master, slave, group, nioFactory);
		if (tairProcessor == null) {
			String groupName = group;
			if (!groupName.endsWith("\0"))  {
				groupName = group + "\0";
			}
			tairProcessor = new NkvProcessor(master, slave, groupName, nioFactory);
		}
		tairProcessor.init();
		logger.info("Nkv3 Client start, connect to : " + this.group);
	}
	
	public void close() {
		if (nioFactory != defaultNioFactory) {
			nioFactory.shutdown();
		}
	}
	
	public static void shutdown() {
		NkvProcessor.shutdown();
		defaultNioFactory.releaseExternalResources();
		defaultNioFactory.shutdown();
	}
	
	public Future<Result<List<byte[]>>> getBatchKeys(short ns, Map<SocketAddress, Integer> offsetMap, 
			int limit, long timeout) throws NkvRpcError, NkvFlowLimit {
		Set<NkvResultFutureImpl<DumpKeyResponse, Result<List<byte[]>>>> futureSet 
			= new HashSet<NkvResultFutureImpl<DumpKeyResponse, Result<List<byte[]>>>>();
		//send the request
		for (Entry<SocketAddress, Integer> entry : offsetMap.entrySet()) {
			int offset = entry.getValue();
			if (offset < 0)
				continue;
			SocketAddress addr = entry.getKey();
			logger.debug("getBatchKeys send offset:" + offset);
			DumpKeyRequest request =  DumpKeyRequest.build(ns, offset, limit);
			request.setContext(entry);
			NkvResultFutureImpl<DumpKeyResponse, Result<List<byte[]>>> future = tairProcessor.callDataServerAsync(addr, request, 
					timeout, DumpKeyResponse.class, NkvResultCastFactory.DUMP_KEY);
			//add the future to future set.
			futureSet.add(future);
		}
		//return the future set.
		return new NkvResultFutureListImpl<DumpKeyResponse, byte[]>(futureSet);
	}
	
	public Future<Result<List<Entry<byte[], byte[]>>>> getBatchKVs(short ns, Map<SocketAddress, Integer> offsetMap, 
			int limit, long timeout) throws NkvRpcError, NkvFlowLimit {
		Set<NkvResultFutureImpl<DumpAllResponse, Result<List<Entry<byte[], byte[]>>>>> futureSet 
			= new HashSet<NkvResultFutureImpl<DumpAllResponse, Result<List<Entry<byte[], byte[]>>>>>();
		//send the request
		for (Entry<SocketAddress, Integer> entry : offsetMap.entrySet()) {
			int offset = entry.getValue();
			if (offset < 0)
				continue;
			SocketAddress addr = entry.getKey();
			DumpAllRequest request =  DumpAllRequest.build(ns, offset, limit);
			logger.debug("getBatchKVs send offset:" + offset);
			CompressContext context = new CompressContext(entry);
			request.setContext(context);
			NkvResultFutureImpl<DumpAllResponse, Result<List<Entry<byte[], byte[]>>>> future = tairProcessor.callDataServerAsync(addr, request, 
					timeout, DumpAllResponse.class, NkvResultCastFactory.DUMP_ALL);
			//add the future to future set.
			futureSet.add(future);
		}
		//return the future set.
		return new NkvResultFutureListImpl<DumpAllResponse, Entry<byte[], byte[]>>(futureSet);
	}

	private NkvResultFutureImpl<ReturnResponse, Result<Void>> putAsyncImpl(short ns, byte[] pkey, byte[] skey, int keyFlag, byte[] value, int valueFlag, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) {
			opt = defaultOptions;
		}
		//OK!!!
		PutRequest request = PutRequest.build(ns, pkey, skey, keyFlag, value, valueFlag, opt, compressOpt);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.PUT);
	}
	
	public NkvResultFutureImpl<ReturnResponse, Result<Void>> putAsync(short ns, byte[] key, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		//why ?
		int keyFlag = (opt != null && opt.getRequestOption() != null && opt.getRequestOption().getVersion() != 0)  ? 1 : 0;
		return putAsyncImpl(ns, key, null, keyFlag, value, 0, opt);
	}
	
	private Future<Result<Void>> putIfNoExistAsyncImpl(short ns, byte[] pkey, byte[] skey, int keyFlag, byte[] value, int valueFlag, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) {
			opt = defaultOptions;
		}
		//OK!!!
		PutIfNoExistRequest request = PutIfNoExistRequest.build(ns, pkey, skey, keyFlag, value, valueFlag, opt, compressOpt);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.PUT);
	}
	
	public Future<Result<Void>> putIfNoExistAsync(short ns, byte[] key, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		//why ?
		int keyFlag = (opt != null && opt.getRequestOption() != null && opt.getRequestOption().getVersion() != 0)  ? 1 : 0;
		return putIfNoExistAsyncImpl(ns, key, null, keyFlag, value, 0, opt);
	}
	
	private NkvResultFutureImpl<GetResponse, Result<byte[]>> getAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		else if (opt.getExpire() != 0)
			return touchAsync(ns, pkey, skey, opt);
		//OK!!!
		GetRequest request = GetRequest.build(ns, pkey, skey);
		CompressContext context = new CompressContext((short)(skey != null ? pkey.length : 0));
		request.setContext(context);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, NkvResultCastFactory.GET);
	}
	
	private NkvResultFutureImpl<GetResponse, Result<byte[]>> touchAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		TouchRequest request = TouchRequest.build(ns, pkey, skey, opt.getExpire());
		CompressContext context = new CompressContext((short)(skey != null ? pkey.length : 0));
		request.setContext(context);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, NkvResultCastFactory.GET);
	}

	public NkvResultFutureImpl<GetResponse, Result<byte[]>> getAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return getAsyncImpl(ns, key, null, opt);
	}
	
	private Future<Result<Void>> deleteLocalAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		DeleteRequest request = DeleteRequest.build(ns, pkey, skey);
		request.setContext((short)(skey != null ? pkey.length : 0));
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.DELETE);
	}
	
	public NkvResultFutureSetImpl<GetResponse, byte[], ResultMap<String, Result<byte[]>>> batchGetAsync(short ns, final List<byte[]> keys, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		if (keys == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		Map<SocketAddress, List<byte[]>> batch = tairProcessor.matchDataServer(keys);

		Set<NkvResultFutureImpl<GetResponse, Result<ResultMap<String, Result<byte[]>>>>> futureSet = new HashSet<NkvResultFutureImpl<GetResponse, Result<ResultMap<String, Result<byte[]>>>>>();
		//send the request
		for (SocketAddress addr : batch.keySet()) {
			List<byte[]> valList = batch.get(addr);
			GetRequest request =  GetRequest.build(ns, valList, opt);
			CompressContext context = new CompressContext(valList);
			request.setContext(context);
			NkvResultFutureImpl<GetResponse, Result<ResultMap<String, Result<byte[]>>>> future = tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, NkvResultCastFactory.BATCH_GET);
			//add the future to future set.
			futureSet.add(future);
		}
		//return the future set.
		return new NkvResultFutureSetImpl<GetResponse, byte[], ResultMap<String, Result<byte[]>>>(futureSet);
	}

	private Future<Result<Integer>> addCountAsyncImpl(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		IncDecRequest request = IncDecRequest.build(ns, pkey, skey, value, defaultValue, opt);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), IncDecResponse.class, NkvResultCastFactory.ADD_COUNT);
	}
	
	/*private Future<Result<Integer>> addCountBoundedAsyncImpl(short ns, byte[] pkey, byte[] skey, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		BoundedIncDecRequest request = BoundedIncDecRequest.build(ns, pkey, skey, value, defaultValue, lowBound, upperBound, opt);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), IncDecResponse.class, NkvResultCastFactory.ADD_COUNT_BOUNDED);
	}*/
	
	public NkvResultFutureImpl<ReturnResponse, Result<Void>> setCountAsync(short ns, byte[] key, int count, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		byte[] incValue = NkvUtil.encodeCountValue(count);
		return putAsyncImpl(ns, key, null, 0, incValue, NkvConstant.NKV_ITEM_FLAG_ADDCOUNT, opt);
	}

	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return addCountAsyncImpl(ns, key, null, value, defaultValue, opt);
	}
//	public Future<Result<Integer>> incrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
//		return addCountBoundedAsyncImpl(ns, key, null, value, defaultValue, lowBound, upperBound, opt);
//	}
	
	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return addCountAsyncImpl(ns, key, null, -value, defaultValue, opt);
	}
//	public Future<Result<Integer>> decrAsync(short ns, byte[] key, int value, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
//		return addCountBoundedAsyncImpl(ns, key, null, -value, defaultValue, lowBound, upperBound, opt);
//	}
	
	private Future<Result<Void>> lockKeyAsync(short ns, byte[] key, int lockType, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		//OK!!!
		LockRequest request = LockRequest.build(ns, key, lockType);
		SocketAddress addr = tairProcessor.matchDataServer(key);
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.LOCK_KEY);
	}
	
	public Future<Result<Void>> lockAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return lockKeyAsync(ns, key, LockRequest.LOCK_VALUE, opt);
	}
	
	public Future<Result<Void>> unlockAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return lockKeyAsync(ns, key, LockRequest.UNLOCK_VALUE, opt);
	}
	
	private Future<Result<byte[]>> getHiddenAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit { 
		if (opt == null) 
			opt = defaultOptions;
		GetHiddenRequest request = GetHiddenRequest.build(ns, pkey, skey);
		CompressContext context = new CompressContext((short)(skey != null ? pkey.length : 0));
		request.setContext(context);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), GetResponse.class, NkvResultCastFactory.GET_HIDDEN);
	}

	public Future<Result<byte[]>> getHiddenAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit { 
		return getHiddenAsyncImpl(ns, key, null, opt);
	}

	private Future<Result<Void>> hideLocalAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		HideRequest request = HideRequest.build(ns, pkey, skey);
		SocketAddress addr = null ;
		if (skey != null) {
			addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		}
		else {
			addr = tairProcessor.matchDataServer(pkey);
		}
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.HIDE);
	}

	public NkvResultFutureImpl<ReturnResponse, Result<Void>> prefixPutAsync(short ns, byte[] pkey, byte[] skey, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return putAsyncImpl(ns, pkey, skey, 0, value, 0, opt);
	}
	
	public Future<Result<Void>> prefixPutIfNoExistAsync(short ns, byte[] pkey, byte[] skey, byte[] value, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return putIfNoExistAsyncImpl(ns, pkey, skey, 0, value, 0, opt);
	}

	public NkvResultFutureImpl<GetResponse, Result<byte[]>> prefixGetAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null) 
			opt = defaultOptions;
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		return getAsyncImpl(ns, pkey, skey, opt);
	}
	
	public NkvResultFutureImpl<ReturnResponse, Result<Void>> prefixSetCountAsync(short ns, byte[] pkey, byte[] skey, int count, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		byte[] incValue = NkvUtil.encodeCountValue(count);
		return putAsyncImpl(ns, pkey, skey, 0, incValue, NkvConstant.NKV_ITEM_FLAG_ADDCOUNT, opt);
	}
	
	public Future<Result<byte[]>> prefixGetHiddenAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		return getHiddenAsyncImpl(ns, pkey, skey, opt);
	}

	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		return addCountAsyncImpl(ns, pkey, skey, count, defaultValue, opt);
	}
//	public Future<Result<Integer>> prefixIncrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
//		if (skey == null) {
//			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
//		}
//		return addCountBoundedAsyncImpl(ns, pkey, skey, count, defaultValue, lowBound, upperBound, opt);
//	}
	
	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		return addCountAsyncImpl(ns, pkey, skey, -count, defaultValue, opt);
	}
//	public Future<Result<Integer>> prefixDecrAsync(short ns, byte[] pkey, byte[] skey, int count, int defaultValue, int lowBound, int upperBound, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
//		if (skey == null) {
//			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
//		}
//		return addCountBoundedAsyncImpl(ns, pkey, skey, -count, defaultValue, lowBound, upperBound, opt);
//	}
	
	private Future<Result<Void>> hideByProxyAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		HideByProxyRequest request = HideByProxyRequest.build(ns, pkey, skey, group);
		request.setContext((short)(skey != null ? pkey.length : 0));
		return tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.HIDE_BY_PROXY);
	}
	
	public Future<Result<Void>> hideByProxyAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = hideByProxyAsyncImpl(ns, key, null, opt);
		if (future == null)
			return hideLocalAsyncImpl(ns, key, null, opt);
		return future;
	}
	
	public Future<Result<Void>> prefixHideByProxyAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		Future<Result<Void>> future = hideByProxyAsyncImpl(ns, pkey, skey, opt);
		if (future == null)
			return hideLocalAsyncImpl(ns, pkey, skey, opt);
		return future;
	}
	
	private Future<Result<Void>> deleteByProxyAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		if (opt == null) {
			opt = defaultOptions;
		}
		//OK!!!
		InvalidByProxyRequest request = InvalidByProxyRequest.build(ns, pkey, skey, group);
		request.setContext((short)(skey != null ? pkey.length : 0));
		return tairProcessor.callInvalidServerAsync(request, opt.getTimeout(), ReturnResponse.class, NkvResultCastFactory.INVALID);	 
	}
	public Future<Result<Void>> invalidByProxyAsync(short ns, byte[] key, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		return invalidByProxyAsyncImpl(ns, key, null, opt);
	}
	
	private Future<Result<Void>> invalidByProxyAsyncImpl(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		Future<Result<Void>> future = deleteByProxyAsyncImpl(ns, pkey, skey, opt);
		if (future == null) 
			return deleteLocalAsyncImpl(ns, pkey, skey, opt);
		return future;
	}
	
	public Future<Result<Void>> prefixInvalidByProxyAsync(short ns, byte[] pkey, byte[] skey, NkvOption opt) throws NkvRpcError, NkvFlowLimit, NkvTimeout, InterruptedException {
		if (skey == null) {
			throw new IllegalArgumentException(NkvConstant.KEY_NOT_AVAILABLE);
		}
		return invalidByProxyAsyncImpl(ns, pkey, skey, opt);
	}

//	public Future<ResultMap<byte[], Result<Integer>>> prefixDecrMultiAsync(short ns, byte[] pkey, Map<byte[], Counter> skv, int lowBound, int upperBound, NkvOption opt)  throws NkvRpcError, NkvFlowLimit {
//		Map<byte[], Counter> skvTemp = new HashMap<byte[], Counter>();
//		for (Map.Entry<byte[], Counter> e : skv.entrySet()) {
//			skvTemp.put(e.getKey(), new Counter(-e.getValue().getValue(), e.getValue().getInitValue(), e.getValue().getExpire()));
//		}
//		return prefixAddCountBoundedMultiAsync(ns, pkey, skvTemp, lowBound, upperBound, opt);
//	}
	
	/*public Future<Result<List<Pair<byte[], Result<byte[]>>>>> getRangeAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		RangeRequest request = RangeRequest.build(ns, pkey, begin, end, offset, maxCount, (reverse ? NkvConstant.RANGE_ALL_REVERSE : NkvConstant.RANGE_ALL), opt);
		request.setContext(pkey);
		SocketAddress addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), RangeResponse.class, NkvResultCastFactory.GET_RANGE);
	}*/

	/*public Future<Result<List<Result<byte[]>>>> deleteRangeAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return operateRangeKeyOrValueAsyncImpl(ns, pkey, begin, end, offset, maxCount, (reverse ? NkvConstant.RANGE_DEL_REVERSE : NkvConstant.RANGE_DEL), opt);
	}*/

	/*private Future<Result<List<Result<byte[]>>>> operateRangeKeyOrValueAsyncImpl(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, short type, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		RangeRequest request = RangeRequest.build(ns, pkey, begin, end, offset, maxCount, type, opt);
		request.setContext(pkey);
		SocketAddress addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		return tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), RangeResponse.class, NkvResultCastFactory.GET_RANGE_KEY); 
	}*/
	
	/*public Future<Result<List<Result<byte[]>>>> getRangeKeyAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return operateRangeKeyOrValueAsyncImpl(ns, pkey, begin, end, offset, maxCount, (reverse ? NkvConstant.RANGE_KEY_ONLY_REVERSE : NkvConstant.RANGE_KEY_ONLY), opt);
	}
	
	public Future<Result<List<Result<byte[]>>>> getRangeValueAsync(short ns, byte[] pkey, byte[] begin, byte[] end, int offset, int maxCount, boolean reverse, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		return operateRangeKeyOrValueAsyncImpl(ns, pkey, begin, end, offset, maxCount, (reverse ? NkvConstant.RANGE_VALUE_ONLY_REVERSE : NkvConstant.RANGE_VALUE_ONLY), opt);
	}*/
	
	public Future<Result<Map<String, String>>> getStatAsync(int qtype, String groupName, long serverId, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		if (opt == null)
			opt = defaultOptions;
		
		if (!groupName.endsWith("\0"))
			groupName = groupName + "\0";
		//OK!!!
		QueryInfoRequest request = QueryInfoRequest.build(qtype, groupName, serverId);
		SocketAddress addr = NkvUtil.cast2SocketAddress(master) ;
		return tairProcessor.callConfigServerAsync(addr, request, opt.getTimeout(), QueryInfoResponse.class, NkvResultCastFactory.QUERY_INFO);
	}

	 /*public Future<ResultMap<byte[], Result<byte[]>>> simplePrefixGetMultiAsync(short ns, byte[] pkey, List<byte[]> skeys, NkvOption opt) throws NkvRpcError, NkvFlowLimit {
		 if (opt == null)
			opt = defaultOptions;
		 SimplePrefixGetMultiRequest request = SimplePrefixGetMultiRequest.build(ns);
		 request.addKeys(pkey, skeys);
		 SocketAddress addr = tairProcessor.matchDataServer(NkvConstant.PREFIX_KEY_TYPE, pkey);
		 Set<NkvResultFutureImpl<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>> futureSet = new HashSet<NkvResultFutureImpl<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>>>();
			
		 NkvResultFutureImpl<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> future =  tairProcessor.callDataServerAsync(addr, request, opt.getTimeout(), SimplePrefixGetMultiResponse.class, NkvResultCastFactory.SIMPLE_PREFIX_GET_MULTI);
		 futureSet.add(future);
		 return new NkvResultFutureSetImpl<SimplePrefixGetMultiResponse, byte[], ResultMap<byte[], Result<byte[]>>>(futureSet); 
	 }*/
			
		 
	public void notifyFuture(Future<?> future, Object ctx) throws NkvQueueOverflow {
		NkvResultFuture<?> rfuture = (NkvResultFuture<?>)future;
		rfuture.setContext(ctx);
		if (notifyQueue.size() >= maxNotifyQueueSize) {
			throw new NkvQueueOverflow("blocking queue is overflow.");
		}
		rfuture.futureNotify(notifyQueue);
	}
	
	public NotifyFuture poll(long timeout, TimeUnit unit) throws InterruptedException {
		NkvResultFuture<?> future = notifyQueue.poll(timeout, unit);
		if (future == null)
			return null;
		return new NotifyFuture(future, future.getContext());
	}
	
	public NotifyFuture poll() throws InterruptedException {
		NkvResultFuture<?> future = notifyQueue.poll();
		if (future == null)
			return null;
		return new NotifyFuture(future, future.getContext());
	}
	
	public Map<String, String> notifyStat() {
		Map<String, String> stat = new HashMap<String, String>();
    	stat.put("csversion", "" + this.tairProcessor.getServerManager().getConfigVersion());
    	stat.put("csgroup", getGroup());
    	stat.put("csaddress", "" + getMaster() + ", " + getSlave());
    	return stat;
	}
	
	public class CompressContext {
		private CompressOption compressOpt;
		private Object context;
		
		public CompressContext() {
			this.compressOpt = AbstractNkvClient.this.compressOpt;
		}
		
		public CompressContext(Object context) {
			this.compressOpt = AbstractNkvClient.this.compressOpt;
			this.context = context;
		}

		public boolean isUseCompress() {
			return compressOpt.isCompressEnabled();
		}

		public boolean isFastCompress() {
			return compressOpt.isUseFastCompress();
		}

		public Object getContext() {
			return context;
		}
	}
	
	public static class CompressOption {
		private boolean compressEnabled = false;
		private boolean useFastCompress = true;
		private int compressThreshold = NkvConstant.MAX_VALUE_SIZE;
		
		public boolean isCompressEnabled() {
			return compressEnabled;
		}
		public boolean isUseFastCompress() {
			return useFastCompress;
		}
		public int getCompressThreshold() {
			return compressThreshold;
		}
	}
}
