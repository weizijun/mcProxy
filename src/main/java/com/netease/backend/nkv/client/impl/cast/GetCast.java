package com.netease.backend.nkv.client.impl.cast;
import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressContext;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.GetResponse;
import com.netease.backend.nkv.client.util.NkvUtil;

public class GetCast implements NkvResultCast<GetResponse,Result<byte[]>> {

	public Result<byte[]> cast(GetResponse s, Object context) throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof CompressContext)) {
			throw new  NkvCastIllegalContext("context of GetCast.");
		}
		CompressContext cc = (CompressContext) context;
		Result<byte[]> result = null;
		ResultCode code = ResultCode.castResultCode(s.getCode());
		
		if (code.equals(ResultCode.OK) && s.getEntrires().size() > 0) {
			result = s.getEntrires().get(0);
			if (cc.isUseCompress()) {
				byte[] value = s.getEntrires().get(0).getResult();
				byte[] restore = NkvUtil.decompress(value);
				if (restore != null)
					result.setResult(restore);
			}
		}
		else {
			result = new Result<byte[]> ();
		}
		result.setCode(code);
		return result;
	}
}
