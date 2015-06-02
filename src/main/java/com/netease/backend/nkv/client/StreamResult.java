package com.netease.backend.nkv.client;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.AbstractNkvClient;

public abstract class StreamResult<T> {

	protected T result;
	protected ResultCode code;
	protected List<T> resultList;
	protected int cursor;
	protected int currentSize;
	protected AbstractNkvClient client = null;
	protected Map<SocketAddress, Integer> offsetMap = null;
	protected short ns;
	protected long timeout;

	public void setCode(ResultCode code) {
		this.code = code;
	}
	
	public ResultCode getCode() {
		return code;
	}

	public T next() throws NkvRpcError, NkvFlowLimit, InterruptedException, ExecutionException, TimeoutException {
		if (result == null)
			return null;
		T r = result;
		if (++cursor < currentSize) {
			result = resultList.get(cursor);
		}
		else {
			resultList.clear();
			fillNextBatch();
		}
		return r;	
	}
	
	public List<T> nextBatch() throws NkvRpcError, NkvFlowLimit, InterruptedException, ExecutionException, TimeoutException {
		if (result == null)
			return null;
		List<T> lst = null;
		if (cursor == 0) {
			lst = resultList;
		} else if (cursor == currentSize) {
			resultList.clear();
			fillNextBatch();
			lst =  resultList;
		} else 
			lst = resultList.subList(cursor, currentSize);
		fillNextBatch();
		return lst;
	}
	
	public boolean hasNext() {
		return result != null;
	}
	
	protected void fillResult(Result<List<T>> res) throws NkvRpcError {
		ResultCode code = res.getCode();
		if (code != ResultCode.OK) {
			setCode(code);
			throw new NkvRpcError("get error code:" + code);
		}
		this.resultList = res.getResult();
		this.currentSize = resultList.size();
		this.cursor = 0;
		if (resultList.size() > 0)
			this.result = resultList.get(0);
		else
			this.result = null;
	}
	
	protected abstract void fillNextBatch() 
			throws NkvRpcError, NkvFlowLimit, InterruptedException, ExecutionException, TimeoutException;
}
