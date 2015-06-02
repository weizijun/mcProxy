package com.netease.backend.nkv.client.impl.cast;
import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
//import com.netease.backend.nkv.client.packets.dataserver.RangeResponse;
/*
public class GetRangeKeyCast implements NkvResultCast<RangeResponse, Result<List<Result<byte[]>>>> {

	public Result<List<Result<byte[]>>> cast(RangeResponse s, Object context)
			throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof byte[])) {
		throw new  NkvCastIllegalContext("context of GetRangeKeyCast.");
	}
	Result<List<Result<byte[]>>> result = new Result<List<Result<byte[]>>>();
	List<Result<byte[]>> list = s.getResults();
	ResultCode code = ResultCode.castResultCode(s.getCode());
	byte[] key = (byte[]) context;
	result.setCode(code);
	result.setKey(key);
	result.setResult(list);
	result.setFlag(s.getFlag());
	return result;
	}
//	public Result<List<Result<byte[]>>> cast(RangeResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {

//	}	
}*/
