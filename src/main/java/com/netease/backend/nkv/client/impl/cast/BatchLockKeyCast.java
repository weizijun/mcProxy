package com.netease.backend.nkv.client.impl.cast;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;

public class BatchLockKeyCast implements NkvResultCast<ReturnResponse, Result<ResultMap<byte[], Result<Void>>>> {
	public Result<ResultMap<byte[], Result<Void>>> cast(ReturnResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof byte[])) {
			throw new  NkvCastIllegalContext("context of BatchLockKey.");
		}
		byte[] key = (byte[]) context;
		Result<ResultMap<byte[], Result<Void>>> res = new Result<ResultMap<byte[], Result<Void>>>();
		ResultMap<byte[], Result<Void>> resMap = new ResultMap<byte[], Result<Void>>();
		Result<Void> rr = new Result<Void>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		rr.setCode(code);
		rr.setKey(key);
		resMap.setCode(code);
		resMap.put(key, rr);
		res.setResult(resMap);
		res.setCode(code);
		return res;
	}
}
