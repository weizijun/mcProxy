package com.netease.backend.nkv.client.impl.cast;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;

public class BatchPutOldCast implements NkvResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws NkvRpcError {
		if (context == null || !(context instanceof byte[])) {
			throw new  IllegalArgumentException("context of BatchPutOldCast.");
		}
		byte [] key = (byte[]) context;
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>>();
		ResultMap<byte[], Result<Void>> r = new ResultMap<byte[], Result<Void>>();
		Result<Void> rr = new Result<Void>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		rr.setCode(code);
		rr.setKey(key);
		r.put(key, rr);
		r.setCode(code);
		result.setResult(r);
		result.setCode(code);
		return result;
	}
}