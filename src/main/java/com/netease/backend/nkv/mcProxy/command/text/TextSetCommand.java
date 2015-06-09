package com.netease.backend.nkv.mcProxy.command.text;

import net.rubyeye.xmemcached.command.CommandType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.McClientError;
import com.netease.backend.nkv.client.error.McError;
import com.netease.backend.nkv.client.error.McServerError;
import com.netease.backend.nkv.mcProxy.command.ProtocolType;
import com.netease.backend.nkv.mcProxy.command.StorageCommand;

/**
 * @author hzweizijun 
 * @date 2015年6月9日 下午3:33:53
 */
public class TextSetCommand extends StorageCommand {

	public TextSetCommand() {
		protocolType = ProtocolType.Text;
		commandType = CommandType.SET;
		
	}
	
	@Override
	public <T> ChannelBuffer encodeTo(Result<T> result) throws McError {
		if (result.getCode() == ResultCode.OK) {
			ChannelBuffer storedBuffer = ChannelBuffers.copiedBuffer(STORED);
			return storedBuffer;
		} else {
			throw new McServerError();
		}
	}

	@Override
	public void decodeFrom(String[] tokens) throws McError {
		if (tokens.length < 5) {
			throw new McError();
		}

		key = tokens[1];
		flags = Integer.parseInt(tokens[2]);
		exptime = Long.parseLong(tokens[3]);
		valueLen = Integer.parseInt(tokens[4]);
	}

	@Override
	public boolean decodeValue(ChannelBuffer buffer) throws McError {
		if (buffer.readableBytes() < valueLen + 2) {
			return false;
		}
		
		value = new byte[valueLen];
		buffer.readBytes(value);
		byte r = buffer.readByte();
		byte n = buffer.readByte();
				
		if (r != '\r' || n != '\n') {
			throw new McClientError("CLIENT_ERROR bad data chunk");
		}
		
		return true;
	}

}
