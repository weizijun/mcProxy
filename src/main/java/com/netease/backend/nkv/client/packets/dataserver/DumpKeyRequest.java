package com.netease.backend.nkv.client.packets.dataserver;

public class DumpKeyRequest extends DumpAllRequest {

	public static DumpKeyRequest build(short ns, int offset, int limit) {
		DumpKeyRequest req = new DumpKeyRequest();
		req.namespace = ns;
		req.offset = offset;
		req.limit = limit;
		return req;
	}
}
