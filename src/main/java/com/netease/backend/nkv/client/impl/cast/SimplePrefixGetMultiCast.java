package com.netease.backend.nkv.client.impl.cast;
import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.SimplePrefixGetMultiResponse;

public class SimplePrefixGetMultiCast
		implements
		NkvResultCast<SimplePrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> {

	public Result<ResultMap<byte[], Result<byte[]>>> cast(
			SimplePrefixGetMultiResponse s, Object context)
			throws NkvRpcError, NkvCastIllegalContext {
		List<Result<byte[]>> res = s.getResult();
		ResultMap<byte[], Result<byte[]>> rm = new ResultMap<byte[], Result<byte[]>>(); 
		if (res != null) {
			for (Result<byte[]> r : res) {
				if (r.getKey() != null) {
					rm.put(r.getKey(), r);
				}
			}
		}
		Result<ResultMap<byte[], Result<byte[]>>> result = new Result<ResultMap<byte[], Result<byte[]>>>();
		result.setCode(ResultCode.castResultCode(s.getCode()));
		rm.setCode(ResultCode.castResultCode(s.getCode()));
		result.setResult(rm);
		return result;
	}
}
