package com.netease.backend.nkv.client.impl.cast;
import java.util.List;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.NkvClient.Pair;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
//import com.netease.backend.nkv.client.packets.dataserver.RangeResponse;
import com.netease.backend.nkv.client.util.NkvConstant;

/*
public class GetRangeCast implements NkvResultCast<RangeResponse, Result<List<Pair<byte[], Result<byte[]>>>>> {
	public Result<List<Pair<byte[], Result<byte[]>>>> cast(RangeResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof byte[])) {
			throw new  NkvCastIllegalContext("context of GetRangeCast.");
		}
		Result<List<Pair<byte[], Result<byte[]>>>> result = new Result<List<Pair<byte[], Result<byte[]>>>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		result.setCode(code);
		short type = s.getType();
		List<Pair<byte[], Result<byte[]>>> r = s.getOrderedResults();
		if ((type == NkvConstant.RANGE_ALL || type == NkvConstant.RANGE_ALL_REVERSE) && r != null) {
			result.setCode(code);
			result.setResult(r);
			
		}
		result.setCode(code);
		result.setResult(r);
		result.setFlag(s.getFlag());
		return result;
	}
}*/
