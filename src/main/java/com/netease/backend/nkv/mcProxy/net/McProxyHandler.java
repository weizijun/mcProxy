package com.netease.backend.nkv.mcProxy.net;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

import com.netease.backend.nkv.client.rpc.net.NkvRpcPacket;
import com.netease.backend.nkv.mcProxy.McProxyContext;
import com.netease.backend.nkv.mcProxy.command.Command;

public class McProxyHandler extends SimpleChannelHandler {
	private McProxyContext context;

	private static final Logger logger = Logger.getLogger(McProxyHandler.class);

	public McProxyHandler(McProxyContext context) {
		this.context = context;
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
			throws Exception {
		logger.debug("McProxyHandler messageReceived");
		context.messageReceived(ctx.getChannel(), (Command)e.getMessage());
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
