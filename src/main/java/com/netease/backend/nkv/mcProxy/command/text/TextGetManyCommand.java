package com.netease.backend.nkv.mcProxy.command.text;

import java.util.ArrayList;

import net.rubyeye.xmemcached.command.CommandType;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.netease.backend.nkv.client.Result;
import com.netease.backend.nkv.client.ResultMap;
import com.netease.backend.nkv.client.Result.ResultCode;
import com.netease.backend.nkv.client.error.McError;
import com.netease.backend.nkv.mcProxy.command.Command;
import com.netease.backend.nkv.mcProxy.command.ProtocolType;

/**
 * @author hzweizijun 
 * @date 2015年6月10日 下午1:40:38
 */
public class TextGetManyCommand extends Command {

	public TextGetManyCommand() {
		protocolType = ProtocolType.Text;
		commandType = CommandType.GET_MANY;
	}

	
	@Override
	public <T> ChannelBuffer encodeTo(Result<T> result) throws McError {
		ChannelBuffer buffer = ChannelBuffers.dynamicBuffer();
		if (result.getCode() == ResultCode.OK) {
			@SuppressWarnings("unchecked")
			ResultMap<String, Result<byte[]>> getManyResult = (ResultMap<String, Result<byte[]>>) result;
			for (byte[] rkey : keys) {
				Result<byte[]> getResult = getManyResult.get(new String(rkey));

				buffer.writeBytes(VALUE);
				buffer.writeByte(SPACE_BYTE);
				buffer.writeBytes(rkey);
				buffer.writeByte(SPACE_BYTE);
				buffer.writeBytes(String.valueOf(getResult.getFlag()).getBytes());
				buffer.writeByte(SPACE_BYTE);
				buffer.writeBytes(String.valueOf(getResult.getResult().length).getBytes());
				buffer.writeBytes(END_BYTES);
				buffer.writeBytes(getResult.getResult());
				buffer.writeBytes(END_BYTES);
			}
			
		}

		buffer.writeBytes(END);
		return buffer;
	}

	@Override
	public void decodeFrom(String[] tokens) throws McError {
		if (tokens.length <= 2) {
			throw new McError();
		}
		keys = new ArrayList<byte[]>();
		for (int i = 1; i < tokens.length; ++i) {
			getKeys().add(tokens[i].getBytes());
		}
	}

}
