package com.netease.backend.nkv.client.impl.cast;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.common.ReturnResponse;

public class PutCast implements NkvResultCast<ReturnResponse, Result<Void>> {

	public Result<Void> cast(ReturnResponse s, Object context) throws NkvRpcError {
		Result<Void> result = new Result<Void>();
		result.setCode(ResultCode.castResultCode(s.getCode()));
		return result;
	}
}
