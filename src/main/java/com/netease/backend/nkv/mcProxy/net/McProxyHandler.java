package com.netease.backend.nkv.mcProxy.net;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class McProxyHandler extends SimpleChannelHandler {
	private McRpcContext context;

	private static final Logger logger = Logger.getLogger(McProxyHandler.class);

	public McProxyHandler(McRpcContext context) {
		this.context = context;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		logger.debug("McProxyHandler messageReceived");
		ChannelBuffer buffer = (ChannelBuffer)e.getMessage();
		context.messageReceived(ctx.getChannel(), buffer);
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.debug("client connect,clent:" + ctx.getChannel().getRemoteAddress());
		context.channelConnected(ctx.getChannel());
	}
	
	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.debug("client disconnect,clent:" + ctx.getChannel().getRemoteAddress());
		context.channelDisconnected(ctx.getChannel(), e);
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.warn("client exceptionCaught,clent:" + ctx.getChannel().getRemoteAddress(), e.getCause());
		context.exceptionCaught(ctx.getChannel(), e.getCause());
	}
}
