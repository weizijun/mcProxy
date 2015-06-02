package com.netease.backend.nkv.client.impl.cast;
import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.NkvClient.Pair;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.PrefixIncDecResponse;
import com.netease.backend.nkv.client.util.ByteArray;


public class PrefixAddCountBoundedMultiCast implements NkvResultCast<PrefixIncDecResponse, Result<ResultMap<byte[], Result<Integer>>>> {
	public Result<ResultMap<byte[], Result<Integer>>> cast(PrefixIncDecResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		 if (context == null || !(context instanceof Pair<?, ?>)) {
			throw new  NkvCastIllegalContext("context of PrefixAddCountMultiCast.");
		}
	 
		@SuppressWarnings("unchecked")
		Pair<byte[], List<ByteArray>> pair = (Pair<byte[], List<ByteArray>>) context;
		byte[] pkey = pair.first();

		
		Result<ResultMap<byte[], Result<Integer>>> result = new Result<ResultMap<byte[], Result<Integer>>>();
		ResultMap<byte[], Result<Integer>> resultMap = s.getResults();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		if (resultMap == null) {
			resultMap = new ResultMap<byte[], Result<Integer>>(0);
		}
		resultMap.setCode(code);
		resultMap.setKey(pkey);
		result.setResult(resultMap);
		return result;
	}
}
