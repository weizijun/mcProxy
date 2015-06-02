/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.netease.backend.nkv.client.packets.common;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.packets.AbstractResponsePacket;


public class SimpleResponse extends AbstractResponsePacket {
	

    public boolean hasConfigVersion() {
    	return true;
    }
    @Override
    public void decodeFrom(ChannelBuffer buffer) {
        this.resultCode          = buffer.readInt();
    }
  
	public int decodeConfigVersionFrom(ChannelBuffer bb) {
		return bb.readInt();
	}
}
