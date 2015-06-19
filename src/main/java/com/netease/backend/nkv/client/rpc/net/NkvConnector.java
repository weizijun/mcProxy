package com.netease.backend.nkv.client.rpc.net;

import java.net.SocketAddress;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

public final class NkvConnector {

    private ClientBootstrap bootstrap = null;
	private static final long DEFAULT_CONNECT_TIMEOUT = 500L;
	
	public NkvConnector(final NkvRpcContext context, ChannelFactory factory) {
		bootstrap = new ClientBootstrap(factory);
		setConnectTimeout(DEFAULT_CONNECT_TIMEOUT);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				return Channels.pipeline(new NkvDecoder(), new NkvHandler(context));
			}
		});
	}
	

	public void setConnectTimeout(long timeout) {
		bootstrap.setOption("connectTimeoutMillis", timeout);
	}
	
	public ChannelFuture createSession(SocketAddress addr, ChannelFutureListener listener) {
		ChannelFuture future = bootstrap.connect(addr);
		future.addListener(listener);
		return future;
	}
	
	
}
