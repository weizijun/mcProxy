package com.netease.backend.nkv.client.impl.cast;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.AbstractNkvClient.CompressContext;
import com.netease.backend.nkv.client.impl.NkvProcessor.NkvResultCast;
import com.netease.backend.nkv.client.packets.dataserver.DumpAllResponse;
import com.netease.backend.nkv.client.util.NkvUtil;


public class DumpAllCast implements NkvResultCast<DumpAllResponse, Result<List<Entry<byte[], byte[]>>>> {

	@Override
	public Result<List<Entry<byte[], byte[]>>> cast(DumpAllResponse s, Object context)
			throws NkvRpcError, NkvCastIllegalContext {
		if (context == null || !(context instanceof CompressContext))
			throw new IllegalArgumentException("DumpKeyCast.");
		
		CompressContext cc = (CompressContext) context;
		ResultCode code = ResultCode.castResultCode(s.getCode());
		Result<List<Entry<byte[], byte[]>>> result = new Result<List<Entry<byte[], byte[]>>>(code);
		int size = s.getValues().size();
		
		if (code.equals(ResultCode.OK)) {
			result.setResult(s.getValues());
			if (cc.isUseCompress()) {
				for (int i = 0; i < size; i++) {
					Entry<byte[], byte[]> itemEntry = s.getValues().get(i);
					byte[] restore = NkvUtil.decompress(itemEntry.getValue());
					if (restore != null) {
						itemEntry.setValue(restore);
					}
				}
			}
			
			@SuppressWarnings("unchecked")
			Map.Entry<SocketAddress, Integer> entry = (Map.Entry<SocketAddress, Integer>) cc.getContext();
			if (size == 0) 
				entry.setValue(-1);
			else 
				entry.setValue(s.getOffset());
		}
		
		return result;	
	}

	
}
