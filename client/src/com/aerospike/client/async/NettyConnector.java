/*
 * Copyright 2012-2021 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.async;

import java.security.cert.X509Certificate;

import javax.net.ssl.SSLSession;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Log;
import com.aerospike.client.ResultCode;
import com.aerospike.client.admin.AdminCommand;
import com.aerospike.client.cluster.Cluster;
import com.aerospike.client.cluster.Connection;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.Buffer;
import com.aerospike.client.policy.TlsPolicy;
import com.aerospike.client.util.Util;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

/**
 * Create netty connection and place in connection pool.
 * Used for min connections functionality.
 */
public final class NettyConnector extends AsyncConnector {

	private final NettyEventLoop eventLoop;
	private final byte[] dataBuffer;
	private NettyConnection conn;
	private int dataOffset;
	private int receiveSize;

	public NettyConnector(NettyEventLoop eventLoop, Cluster cluster, Node node, AsyncConnector.Listener listener) {
		super(eventLoop, cluster, node, listener);
		this.eventLoop = eventLoop;
		this.dataBuffer = (cluster.getUser() != null) ? new byte[256] : null;
	}

	@Override
	public void createConnection() {
		final InboundHandler handler = new InboundHandler();
		handler.command = this;

		Bootstrap b = new Bootstrap();
		b.group(eventLoop.eventLoop);

		if (eventLoop.parent.isEpoll) {
			b.channel(EpollSocketChannel.class);
		} else if(eventLoop.parent.isKqueue) {
			b.channel(KQueueSocketChannel.class);
		}
		else {
			b.channel(NioSocketChannel.class);
		}
		b.option(ChannelOption.TCP_NODELAY, true);
		b.option(ChannelOption.AUTO_READ, false);

		b.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) {
				if (state != AsyncCommand.CONNECT) {
					// State mismatch. Timeout probably occurred.
					// Close channel.
					ch.close();
					return;
				}

				conn = new NettyConnection(ch);
				node.connectionOpened(eventLoop.index);
				ChannelPipeline p = ch.pipeline();

				if (eventLoop.parent.sslContext != null && !eventLoop.parent.tlsPolicy.forLoginOnly) {
					state = AsyncCommand.TLS_HANDSHAKE;
					//InetSocketAddress address = node.getAddress();
					//p.addLast(eventLoop.parent.sslContext.newHandler(ch.alloc(), address.getHostString(), address.getPort()));
					p.addLast(eventLoop.parent.sslContext.newHandler(ch.alloc()));
				}
				p.addLast(handler);
			}
		});
		b.connect(node.getAddress());
	}

	private void channelActive() {
		if (cluster.getUser() != null) {
			writeAuth();
		}
		else {
			finish();
		}
	}

	private void writeAuth() {
		state = AsyncCommand.AUTH_WRITE;

		AdminCommand admin = new AdminCommand(dataBuffer);
		dataOffset = admin.setAuthenticate(cluster, node.getSessionToken());

		ByteBuf byteBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(dataOffset);
		byteBuffer.clear();
		byteBuffer.writeBytes(dataBuffer, 0, dataOffset);

		ChannelFuture cf = conn.channel.writeAndFlush(byteBuffer);
		cf.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				state = AsyncCommand.AUTH_READ_HEADER;
				dataOffset = 0;
				conn.channel.config().setAutoRead(true);
			}
		});
	}

	private void read(ByteBuf byteBuffer) {
		try {
			switch (state) {
			case AsyncCommand.AUTH_READ_HEADER:
				readAuthHeader(byteBuffer);
				break;

			case AsyncCommand.AUTH_READ_BODY:
				readAuthBody(byteBuffer);
				break;
			}
		}
		finally {
			byteBuffer.release();
		}
	}

	private void readAuthHeader(ByteBuf byteBuffer) {
		int avail = byteBuffer.readableBytes();
		int offset = dataOffset + avail;

		if (offset < 8) {
			byteBuffer.readBytes(dataBuffer, dataOffset, avail);
			dataOffset = offset;
			return;
		}

		// Process authentication header.
		byteBuffer.readBytes(dataBuffer, dataOffset, 8 - dataOffset);
		receiveSize = ((int)(Buffer.bytesToLong(dataBuffer, 0) & 0xFFFFFFFFFFFFL));

		if (receiveSize < 2 || receiveSize > dataBuffer.length) {
			throw new AerospikeException.Parse("Invalid auth receive size: " + receiveSize);
		}

		state = AsyncCommand.AUTH_READ_BODY;
		offset -= 8;
		dataOffset = offset;

		if (offset > 0) {
			byteBuffer.readBytes(dataBuffer, 0, offset);

			if (offset >= receiveSize) {
				parseAuthBody();
			}
		}
	}

	private void readAuthBody(ByteBuf byteBuffer) {
		int avail = byteBuffer.readableBytes();
		int offset = dataOffset + avail;

		if (offset < receiveSize) {
			byteBuffer.readBytes(dataBuffer, dataOffset, avail);
			dataOffset = offset;
			return;
		}
		parseAuthBody();
	}

	private void parseAuthBody() {
		int resultCode = dataBuffer[1] & 0xFF;

		if (resultCode != 0 && resultCode != ResultCode.SECURITY_NOT_ENABLED) {
			// Authentication failed. Session token probably expired.
			// Signal tend thread to perform node login, so future
			// transactions do not fail.
			node.signalLogin();

			// This is a rare event because the client tracks session
			// expiration and will relogin before session expiration.
			// Do not try to login on same socket because login can take
			// a long time and thousands of simultaneous logins could
			// overwhelm server.
			throw new AerospikeException(resultCode);
		}
		finish();
	}

	private final void finish() {
		// Assign normal InboundHandler to connection.
		ChannelPipeline p = conn.channel.pipeline();
		p.removeLast();
		p.addLast(new NettyCommand.InboundHandler());

		conn.channel.config().setAutoRead(false);
		conn.updateLastUsed();
		success();
	}

	@Override
	final void addConnection() {
		node.addAsyncConnector(conn, eventLoop.index);
		conn = null;
	}

	@Override
	final void closeConnection() {
		if (conn != null) {
			node.closeAsyncConnector(conn, eventLoop.index);
			conn = null;
		}
	}

	static class InboundHandler extends ChannelInboundHandlerAdapter {
		NettyConnector command;

		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			// Mark connection ready in regular (non TLS) mode.
			// Otherwise, wait for TLS handshake to complete.
			if (command.state == AsyncCommand.CONNECT) {
				command.channelActive();
			}
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			command.read((ByteBuf)msg);
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (! (evt instanceof SslHandshakeCompletionEvent)) {
				return;
			}

			Throwable cause = ((SslHandshakeCompletionEvent)evt).cause();

			if (cause != null) {
				throw new AerospikeException("TLS connect failed: " + cause.getMessage(), cause);
			}

			TlsPolicy tlsPolicy = command.eventLoop.parent.tlsPolicy;

			String tlsName = command.node.getHost().tlsName;
			SSLSession session = ((SslHandler)ctx.pipeline().first()).engine().getSession();
			X509Certificate cert = (X509Certificate)session.getPeerCertificates()[0];

			Connection.validateServerCertificate(tlsPolicy, tlsName, cert);

			if (command.state == AsyncCommand.TLS_HANDSHAKE) {
				command.channelActive();
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			if (command == null) {
				Log.error("Connection exception: " + Util.getErrorMessage(cause));
				return;
			}
			command.fail(new AerospikeException(cause));
		}
	}
}
