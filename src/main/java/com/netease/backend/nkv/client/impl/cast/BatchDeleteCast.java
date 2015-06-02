package com.netease.backend.nkv.client.impl.cast;

import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;


public class BatchDeleteCast implements NkvResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof List<?>)) {
			throw new  NkvCastIllegalContext("context of BatchDeleteCast.");
		}
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>>();
		ResultMap<byte[], Result<Void>> valueMap = new ResultMap<byte[], Result<Void>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		@SuppressWarnings("unchecked")
		List<byte[]> keys = (List<byte[]>) context;
		for (byte[] key : keys) {
			Result<Void> e = new Result<Void>();
			e.setCode(code);
			valueMap.put(key, e);
		}
		valueMap.setCode(code);
		result.setCode(code);
		result.setResult(valueMap);
		return result;
	}
}
