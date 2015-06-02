package com.netease.backend.nkv.client.impl.cast;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.DumpKeyResponse;


public class DumpKeyCast implements NkvResultCast<DumpKeyResponse, Result<List<byte[]>>> {

	@Override
	public Result<List<byte[]>> cast(DumpKeyResponse s, Object context)
			throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof Map.Entry<?, ?>))
			throw new IllegalArgumentException("DumpKeyCast.");
		ResultCode code = ResultCode.castResultCode(s.getCode());
		Result<List<byte[]>> result = new Result<List<byte[]>>(code);
		result.setResult(s.getValues());
		int size = s.getValues().size();
		
		if (code.equals(ResultCode.OK)) {
			@SuppressWarnings("unchecked")
			Map.Entry<SocketAddress, Integer> entry = (Map.Entry<SocketAddress, Integer>) context;
			if (size == 0) 
				entry.setValue(-1);
			else 
				entry.setValue(s.getOffset());
		}
		return result;	
	}

	
}
