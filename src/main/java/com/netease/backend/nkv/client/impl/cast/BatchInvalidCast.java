package com.netease.backend.nkv.client.impl.cast;

import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;


public class BatchInvalidCast implements NkvResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof List<?>)) {
			throw new  NkvCastIllegalContext("context of BatchInvalidCast.");
		}
		@SuppressWarnings("unchecked")
		List<byte[]> keys = (List<byte[]>)context;
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>> ();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		for (byte[] key : keys) {
			Result<Void> r = new Result<Void>();
			r.setCode(code);
			r.setKey(key);
			resMap.put(key, r);
		}
		result.setResult(resMap);
		resMap.setCode(code);
		result.setCode(code);
		return result;
	}

}
