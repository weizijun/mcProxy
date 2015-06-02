package com.netease.backend.nkv.client.impl;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.StreamResult;
import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvRpcError;

public class KVDump extends StreamResult<Entry<byte[], byte[]>> {
	
	public KVDump(short ns, AbstractNkvClient client, Result<List<Entry<byte[], byte[]>>> firstResult, 
			Map<SocketAddress, Integer> offsetMap, long timeout) throws NkvRpcError {
		this.client = client;
		this.ns = ns;
		this.offsetMap = offsetMap;
		this.timeout = timeout;
		fillResult(firstResult);
		this.setCode(ResultCode.OK);
	}
	
	@Override
	protected void fillNextBatch() 
			throws NkvRpcError, NkvFlowLimit, InterruptedException, ExecutionException, TimeoutException {
		Future<Result<List<Entry<byte[], byte[]>>>>  future = 
				client.getBatchKVs(ns, offsetMap, AbstractNkvClient.getExportBatchNum(), timeout);
		Result<List<Entry<byte[], byte[]>>> res = future.get(timeout, TimeUnit.MILLISECONDS);
		fillResult(res);
	}
	
}
