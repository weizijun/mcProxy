package com.netease.backend.nkv.client.impl.cast;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.IncDecResponse;

public class AddCountCast implements NkvResultCast<IncDecResponse, Result<Integer>> {
	public Result<Integer> cast(IncDecResponse s, Object context) throws NkvRpcError {
		Result<Integer> result = new Result<Integer> ();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		if (code.equals(ResultCode.OK) || code.equals(ResultCode.NOTEXISTS)) {
			result.setResult(s.getValue());
			result.setCode(ResultCode.OK);
		}
		else {
			result.setResult(s.getValue());
			result.setCode(code);
		}	
		return result;
	}
}
