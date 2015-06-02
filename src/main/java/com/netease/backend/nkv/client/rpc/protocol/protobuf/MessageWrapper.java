package com.netease.backend.nkv.client.rpc.protocol.protobuf;

import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;

import com.google.protobuf.Message;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;

public class MessageWrapper implements NkvRpcPacket{
	
	private MessageHeader header;
	private Message message = null;
	private MessageManager manager;
	
	private ChannelBuffer lazyMessageBuffer;
	
	public static MessageWrapper buildWithHeader(ChannelBuffer in, MessageManager manager) throws NkvRpcError {
		MessageWrapper wrapper	 = new MessageWrapper();
		wrapper.header			 = new MessageHeader(in);
		wrapper.manager			 = manager;
		return wrapper;
	}
	
	public static MessageWrapper buildWithBody(int chid, Message body, MessageManager manager) {
		MessageWrapper wrapper 	= new MessageWrapper();
		wrapper.header 			= new MessageHeader(chid, manager.getMessageType(body.getClass()));
		wrapper.message 		= body;
		wrapper.manager		    = manager;
		return wrapper;
	}
	
	public boolean assignBodyBuffer(ChannelBuffer in) {
		if (in.readableBytes() < header.getMessageLength()) {
			return false;
		}
		lazyMessageBuffer = in.readSlice(header.getMessageLength());
		return true;
	}

	public ChannelBuffer encode() throws NkvRpcError{
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		header.encodeTo(buffer);
		ChannelBufferOutputStream cbos = new ChannelBufferOutputStream(buffer);
		try {
			message.writeTo(cbos);
		} catch (IOException e) {
			throw new NkvRpcError(e);
		}
		header.encodeLength(buffer);
		buffer.resetReaderIndex();
		return buffer;
	}

	public int getChannelSeq() {
		return header.getChannelSeq();
	}

	public Object getBody() {
		return message;
	}

	public void decodeBody() throws NkvException {

		ChannelBufferInputStream cbis = new ChannelBufferInputStream(this.lazyMessageBuffer);
		try {
			message = manager.getMessageClass(header.getMessageType()).mergeFrom(cbis).build();
		
		} catch (Exception e) {
			throw new NkvException(e);
		}
	}

	public int getBodyLength() {
		return header.getMessageLength();
	}

	public int decodeConfigVersion() throws NkvException {
		return 0;
	}

	public boolean hasConfigVersion() {
		return false;
	}

	public int decodeResultCode() throws NkvException {
		return 0;
	}
}
