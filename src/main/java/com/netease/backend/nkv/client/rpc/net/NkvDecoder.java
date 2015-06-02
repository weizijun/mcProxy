package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.rpc.protocol.tair2_3.PacketHeader;

//NkvDecoder��һ��upstream����
public class NkvDecoder extends FrameDecoder{

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws NkvRpcError {
		NkvChannel tc = (NkvChannel)channel.getAttachment();
		NkvRpcPacket packet = tc.getCachedPacketWrapper();
		
		if (packet == null) {
			//���buffer�����ݴ���HEADER_SIZE���򹹽�packet header
			if (buffer.readableBytes() < PacketHeader.HEADER_SIZE) {
				return null;
			}
			
			packet = tc.getPacketFactory().buildWithHeader(buffer);
		}
		//���buffer�е����ݲ�������Body����ô����header��packet����nkvchannel��
		//�ȵ��´�buffer�е������㹻����Bodyʱ����������Ҫ��header��packet����ʱ����ִ��assignBodyBuffer
		if (buffer.readableBytes() < packet.getBodyLength() ) {
			tc.setCachedPacketWrapper(packet);
			return null;
		} else {
			tc.setCachedPacketWrapper(null);
		}
		
		packet.assignBodyBuffer(buffer);
		return packet;
		
	}

}
