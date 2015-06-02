package com.netease.backend.nkv.mcProxy.net;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class NettyHandler extends SimpleChannelHandler {
	private McRpcContext context;

	private static final Logger logger = Logger.getLogger(NettyHandler.class);

	public NettyHandler(McRpcContext context) {
		this.context = context;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		logger.debug("NettyHandler messageReceived");
	}

	@Override
	public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
			throws Exception {
		logger.debug("client connect,clent:" + ctx.getChannel().getRemoteAddress());
	}
	
	@Override
	public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		logger.debug("client disconnect,clent:" + ctx.getChannel().getRemoteAddress());
	}
	
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
			throws Exception {
		logger.debug("client exceptionCaught,clent:" + ctx.getChannel().getRemoteAddress());
	}
}
