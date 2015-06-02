package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.rpc.protocol.tair2_3.PacketHeader;

//NkvDecoder是一个upstream流程
public class NkvDecoder extends FrameDecoder{

	@Override
	protected Object decode(ChannelHandlerContext ctx, Channel channel,
			ChannelBuffer buffer) throws NkvRpcError {
		NkvChannel tc = (NkvChannel)channel.getAttachment();
		NkvRpcPacket packet = tc.getCachedPacketWrapper();
		
		if (packet == null) {
			//如果buffer中数据大于HEADER_SIZE，则构建packet header
			if (buffer.readableBytes() < PacketHeader.HEADER_SIZE) {
				return null;
			}
			
			packet = tc.getPacketFactory().buildWithHeader(buffer);
		}
		//如果buffer中的数据不够构建Body，那么将带header的packet传给nkvchannel，
		//等到下次buffer中的数据足够构建Body时，将不再需要带header的packet，此时可以执行assignBodyBuffer
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
