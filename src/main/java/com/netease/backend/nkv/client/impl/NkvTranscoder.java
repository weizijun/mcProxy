package com.netease.backend.nkv.client.impl;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class NkvTranscoder {
	
	public static NkvTranscoder defaultTranscoder = new NkvTranscoder();

	ChannelBuffer encodeTo(Object obj) throws RuntimeException {
		return ChannelBuffers.wrappedBuffer((byte[])obj);
	}
	
	public Object decodeTo(ChannelBuffer buffer) throws RuntimeException {
		return buffer.array();
	}
}
