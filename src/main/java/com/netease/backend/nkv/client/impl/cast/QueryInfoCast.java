package com.netease.backend.nkv.client.impl.cast;

import java.util.Map;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.configserver.QueryInfoResponse;
public class QueryInfoCast implements NkvResultCast<QueryInfoResponse,Result<Map<String, String>>> {

	public Result<Map<String, String>> cast(QueryInfoResponse s, Object context)
			throws NkvRpcError, NkvCastIllegalContext {
		Result<Map<String, String>> result = new Result<Map<String, String>>();
		result.setCode(ResultCode.OK);
		result.setResult(s.getInfoMap());
		return result;
	}


}
