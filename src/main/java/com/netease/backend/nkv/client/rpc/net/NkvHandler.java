package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class NkvHandler extends SimpleChannelHandler {
	
	private NkvRpcContext context;
	
	public NkvHandler(NkvRpcContext context) {
		this.context = context;
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		this.context.exceptionCaught(ctx.getChannel(), e.getCause());
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		this.context.messageReceived(ctx.getChannel(), (NkvRpcPacket)e.getMessage());
	}
	
	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		this.context.channelDisconnected(ctx.getChannel());
	}
}
