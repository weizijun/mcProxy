package com.netease.backend.nkv.client.impl.cast;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.BatchPutResponse;

public class BatchPutCast implements NkvResultCast<BatchPutResponse, Result<ResultMap<byte[], Result<Void>>>> {

	public Result<ResultMap<byte[], Result<Void>>> cast(BatchPutResponse s,
			Object context) throws NkvRpcError, NkvCastIllegalContext {
		return null;
	}
	
}
