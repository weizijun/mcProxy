package com.netease.backend.nkv.client.impl;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.error.NkvCastIllegalContext;
import com.netease.backend.nkv.client.error.NkvException;
import com.netease.backend.nkv.client.error.NkvFlowLimit;
import com.netease.backend.nkv.client.error.NkvRpcError;
import com.netease.backend.nkv.client.impl.invalid.InvalidServer;
import com.netease.backend.nkv.client.packets.AbstractRequestPacket;
import com.netease.backend.nkv.client.packets.AbstractResponsePacket;
import com.netease.backend.nkv.client.rpc.future.NkvResultFutureImpl;
import com.netease.backend.nkv.client.rpc.net.NkvBgWorker;
import com.netease.backend.nkv.client.rpc.net.NkvFuture;
import com.netease.backend.nkv.client.rpc.net.NkvRpcContext;
import com.netease.backend.nkv.client.rpc.net.NkvRpcPacketFactory;
import com.netease.backend.nkv.client.rpc.protocol.tair2_3.PacketFactory;

public class NkvProcessor {
	//private static Logger logger = LoggerFactory.getLogger(NkvProcessor.class);
	private NkvRpcContext context = null;
	
	protected ServerManager serverManager;
	private NkvRpcPacketFactory packet2_3Factory = PacketFactory.getInstance();
	
	private static ConfigServerUpdater csUpdater		  = new ConfigServerUpdater();
	private static NkvBgWorker		   timeoutCheckWorker = new NkvBgWorker();
	private String group = null;
	static {
		timeoutCheckWorker.start();
		csUpdater.start();
	}
	
	public static void shutdown() {
		timeoutCheckWorker.interrupt();
		csUpdater.shutdown();
	}
	public NkvProcessor(String master, String slave, String group, NioClientSocketChannelFactory nioFactory) {
		this.group = group;
		context = new NkvRpcContext(nioFactory, timeoutCheckWorker, this.group);
		serverManager = new ServerManager(master, slave, group, context, csUpdater);
		context.setServerManager(serverManager);
		
	}
	
	public void init() throws NkvException {
		serverManager.init();
	}
	 	
	public ServerManager getServerManager() {
		return serverManager;
	}

	public SocketAddress matchDataServer(byte[] prefix, byte[] key) throws NkvRpcError {
		SocketAddress addr = serverManager.findDataServer(prefix, key);
		if (addr == null) {
			serverManager.maybeForceCheckVersion();
			throw new NkvRpcError("not find any DataServer");
		}
		return addr;
	}
	public SocketAddress matchDataServer(byte[] key) throws NkvRpcError {
		return matchDataServer(null, key);
	}
	 
	public Map<SocketAddress, List<byte[]>> matchDataServer(byte[] prefix, List<byte[]> keys) throws NkvRpcError{
		HashMap<SocketAddress, List<byte[]>> result = new HashMap<SocketAddress, List<byte[]>>();
		for (byte[] key : keys) {
			SocketAddress addr = serverManager.findDataServer(prefix, key);
			if (addr == null) {
				throw new NkvRpcError("not find any DataServer");
			}
			List<byte[]> keyList = result.get(addr);
			if (keyList == null) {
				keyList = new ArrayList<byte[]> ();
				keyList.add(key);
				result.put(addr, keyList);
			} else {
				keyList.add(key);
			}
		}
		return result;	
	}
	public Map<SocketAddress, List<byte[]>> matchDataServer(List<byte[]> keys) throws NkvRpcError{
		return matchDataServer(null, keys);
	}
	
	public <S extends AbstractResponsePacket, T> NkvResultFutureImpl<S, Result<T>> callDataServerAsync(SocketAddress addr, AbstractRequestPacket req, long timeout, Class<S> respCls, NkvResultCast<S, Result<T>> cast) 
			throws NkvRpcError, NkvFlowLimit {
		if (addr == null) {
			throw new NkvRpcError("Message: not find any DataServer, GroupName: " + this.group +", AREA: " + req.getNamespace());
		}
		return context.callAsync(addr, req, timeout, respCls, packet2_3Factory, cast);
	}
	
	public NkvFuture callDataServerAsync(SocketAddress addr, AbstractRequestPacket req, long timeout) 
			throws NkvRpcError, NkvFlowLimit {
		if (addr == null) {
			throw new NkvRpcError("Message: not find any DataServer, GroupName: " + this.group +", AREA: " + req.getNamespace());
		}
		return context.callAsync(addr, req, timeout, packet2_3Factory);
	}

	public <S extends AbstractResponsePacket, T> NkvResultFutureImpl<S, Result<T>> callInvalidServerAsync(AbstractRequestPacket req, long timeout, Class<S> respCls, NkvResultCast<S, Result<T>> cast) 
			throws NkvRpcError, NkvFlowLimit {
		InvalidServer invalServer = serverManager.chooseInvalidServer();
		if (invalServer == null) {
			//throw new NkvRpcError("not find any InvalServer.");
			return null;
		}
		return context.callInvalServerAsync(invalServer, req, timeout, respCls, packet2_3Factory, cast);
		 
	}
	
	public <S extends AbstractResponsePacket, T> NkvResultFutureImpl<S, Result<T>> callConfigServerAsync(SocketAddress addr, AbstractRequestPacket req, long timeout, Class<S> respCls, NkvResultCast<S, Result<T>> cast) 
			throws NkvRpcError, NkvFlowLimit {
		if (addr == null) {
			throw new NkvRpcError("ConfigServer's address is null, GroupName: " + this.group);
		}
		return context.callAsync(addr, req, timeout, respCls, packet2_3Factory, cast);
	}

	public interface NkvResultCast<S, T> {
		public T cast(S s, Object context) throws NkvRpcError, NkvCastIllegalContext;
	}
	
	public List<SocketAddress> getDsList() {
		return serverManager.getDsList();
	}
}
