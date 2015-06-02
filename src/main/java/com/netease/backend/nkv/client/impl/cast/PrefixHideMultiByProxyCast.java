package com.netease.backend.nkv.client.impl.cast;

import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.NkvClient.Pair;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;
public class PrefixHideMultiByProxyCast implements NkvResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof Pair<?, ?>)) {
			throw new  NkvCastIllegalContext("context of PrefixHideMultiByProxyCast.");
		}
		@SuppressWarnings("unchecked")
		Pair<byte[], List<byte[]>> pair = (Pair<byte[], List<byte[]>>) context;
		byte[] pkey = pair.first();
		List<byte[]> skeys = pair.second();
		
		Result<ResultMap<byte[], Result<Void>>> result = new Result<ResultMap<byte[], Result<Void>>> ();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>> ();
		
		ResultCode code = ResultCode.castResultCode(s.getCode());
		for (byte[] key : skeys) {
			Result<Void> r = new Result<Void>();
			r.setCode(code);
			resMap.put(key, r);
		}
		resMap.setCode(code);
		resMap.setKey(pkey);
		result.setResult(resMap);
		result.setCode(code);
		return result;
	}

}
