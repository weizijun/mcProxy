package com.netease.backend.nkv.client.rpc.net;

import org.jboss.netty.buffer.ChannelBuffer;

import com.netease.backend.nkv.client.error.NkvRpcError;
//��������ͨ��Э����Ĵ�������ģ�ͣ����ڲ���NkvRpcPacket
public interface NkvRpcPacketFactory {
    //����channelID��request��������ͨ��Э�������Ҫ���ڷ���
	public NkvRpcPacket buildWithBody(int chid, Object body);
	//���ݽ��յ���buffer���ݽ�������NkvRpcPacket����Ҫ�����հ���������
	public NkvRpcPacket buildWithHeader(ChannelBuffer in) throws NkvRpcError;
}
