package com.netease.backend.nkv.client.impl.cast;
import java.util.Map;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressContext;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.PrefixGetMultiResponse;
import com.netease.backend.nkv.client.util.NkvUtil;



public class BatchPrefixGetMultiCast implements NkvResultCast<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>> {
	public Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> cast(PrefixGetMultiResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof CompressContext)) {
			throw new  NkvCastIllegalContext("context of BatchPrefixGetMultiCast.");
		}
		CompressContext compressContext = (CompressContext) context;
		Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>> result = new Result<ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>>();
		ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>> e = new ResultMap<byte[], Result<Map<byte[], Result<byte[]>>>>(1);
		ResultCode code = ResultCode.castResultCode(s.getCode());
		ResultMap<byte[], Result<byte[]>> datas = s.getResults();
		if (compressContext.isUseCompress()) {
			for (Result<byte[]> rs : datas.values()) {
				byte[] v = rs.getResult();
				byte[] restored = NkvUtil.decompress(v);
				if (restored != null)
					rs.setResult(restored);
			}
		}
		Result<Map<byte[], Result<byte[]>>> r = new Result<Map<byte[], Result<byte[]>>>();
		
		result.setCode(code);
		r.setCode(code);
		r.setKey(datas.getKey());
		r.setResult(datas.getResult());
		e.put(r.getKey(), r);
		e.setCode(code);
		result.setResult(e);
		return result;
	}
}
