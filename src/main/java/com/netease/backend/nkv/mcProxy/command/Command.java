package com.netease.backend.nkv.mcProxy.command;



import java.util.List;

import net.rubyeye.xmemcached.command.CommandType;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.error.McError;

/**
 * @author hzweizijun 
 * @date 2015年6月8日 上午10:23:15
 */
public abstract class Command {
	protected final static String NOREPLAY = "noreply";
	protected final static byte[] STORED = "STORED\r\n".getBytes();
	protected final static byte[] DELETED = "DELETED\r\n".getBytes();
	protected final static byte[] END = "END\r\n".getBytes();
	protected final static byte[] END_BYTES = "\r\n".getBytes();
	protected final static byte[] VALUE = "VALUE".getBytes();
	
	protected final static byte SPACE_BYTE = ' ';

	protected byte[] key;
	protected List<byte[]> keys;
	protected byte[] value;
	protected CommandType commandType;
	protected int flags;
	protected long exptime = 0;
	protected int valueLen = 0;
	protected ProtocolType protocolType;
	protected boolean noreply = false;
	
	public abstract <T> ChannelBuffer encodeTo(Result<T> result) throws McError;
	public abstract void decodeFrom(String[] tokens) throws McError;
	
	public byte[] getKey() {
		return key;
	}
	public byte[] getValue() {
		return value;
	}
	public void setValue(byte[] value) {
		this.value = value;
	}
	public CommandType getCommandType() {
		return commandType;
	}
	public List<byte[]> getKeys() {
		return keys;
	}
}
