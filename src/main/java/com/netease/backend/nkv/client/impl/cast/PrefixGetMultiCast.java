package com.netease.backend.nkv.client.impl.cast;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressContext;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.PrefixGetMultiResponse;
import com.netease.backend.nkv.client.util.NkvUtil;


public class PrefixGetMultiCast implements NkvResultCast<PrefixGetMultiResponse, Result<ResultMap<byte[], Result<byte[]>>>> {
	public Result<ResultMap<byte[], Result<byte[]>>> cast(PrefixGetMultiResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof CompressContext)) {
			throw new  NkvCastIllegalContext("context of PrefixGetMultiCast.");
		}
		CompressContext compressContext = (CompressContext) context;
		Result<ResultMap<byte[], Result<byte[]>>> result = new Result<ResultMap<byte[], Result<byte[]>>>();
		ResultCode code = ResultCode.castResultCode(s.getCode());
		ResultMap<byte[], Result<byte[]>> r = s.getResults();
		if (r == null) {
			 r = new ResultMap<byte[], Result<byte[]>>(0);
		}
		if (compressContext.isUseCompress()) {
			for (Result<byte[]> rs : r.values()) {
				byte[] v = rs.getResult();
				byte[] restored = NkvUtil.decompress(v);
				if (restored != null)
					rs.setResult(restored);
			}
		}
		r.setCode(code);
		result.setCode(code);
		result.setResult(r);
		return result;
	}

}
