/*
 * Copyright 2012-2022 Aerospike, Inc.
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
import com.aerospike.client.cluster.Node.AsyncPool;
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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
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
		this.dataBuffer = (cluster.authEnabled) ? new byte[256] : null;
	}

	@Override
	public void createConnection() {
		state = AsyncCommand.CHANNEL_INIT;

		final InboundHandler handler = new InboundHandler(this);

		Bootstrap b = new Bootstrap();
		NettyCommand.initBootstrap(b, cluster, eventLoop);

		b.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) {
				if (state != AsyncCommand.CHANNEL_INIT) {
					// Timeout occurred. Close channel.
					try {
						ch.close();
					}
					catch (Throwable e) {
					}
					return;
				}

				state = AsyncCommand.CONNECT;
				conn = new NettyConnection(ch);
				node.connectionOpened(eventLoop.index);
				ChannelPipeline p = ch.pipeline();

				if (cluster.tlsPolicy != null && !cluster.tlsPolicy.forLoginOnly) {
					state = AsyncCommand.TLS_HANDSHAKE;

					SslHandler hdl = cluster.nettyTlsContext.createHandler(ch);
					hdl.setHandshakeTimeoutMillis(cluster.connectTimeout);
					p.addLast(hdl);
				}
				p.addLast(handler);
			}
		});
		b.connect(node.getAddress());
	}

	private void channelActive() {
		if (cluster.authEnabled) {
			byte[] token = node.getSessionToken();

			if (token != null) {
				writeAuth(token);
				return;
			}
		}

		finish();
	}

	private void writeAuth(byte[] token) {
		state = AsyncCommand.AUTH_WRITE;

		AdminCommand admin = new AdminCommand(dataBuffer);
		dataOffset = admin.setAuthenticate(cluster, token);

		ByteBuf byteBuffer = PooledByteBufAllocator.DEFAULT.directBuffer(dataOffset);
		byteBuffer.clear();
		byteBuffer.writeBytes(dataBuffer, 0, dataOffset);

		ChannelFuture cf = conn.channel.writeAndFlush(byteBuffer);
		cf.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) {
				if (state == AsyncCommand.AUTH_WRITE) {
					state = AsyncCommand.AUTH_READ_HEADER;
					dataOffset = 0;
					conn.channel.config().setAutoRead(true);
				}
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

			default:
				// Timeout occurred. Cancel.
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
			// commands do not fail.
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
		try {
			// Assign normal InboundHandler to connection.
			SocketChannel channel = conn.channel;
			channel.config().setAutoRead(false);

			ChannelPipeline p = channel.pipeline();
			p.removeLast();

			if (cluster.keepAlive == null) {
				p.addLast(new NettyCommand.InboundHandler());
			}
			else {
				AsyncPool pool = node.getAsyncPool(eventState.index);
				p.addLast(new NettyCommand.InboundHandler(pool));
			}

			conn.updateLastUsed();
			success();
		}
		catch (Throwable e) {
			Log.error("NettyConnector fatal error: " + Util.getStackTrace(e));
			throw e;
		}
	}

	@Override
	final boolean addConnection() {
		boolean ret = node.putAsyncConnection(conn, eventLoop.index);
		conn = null;
		return ret;
	}

	@Override
	final void closeConnection() {
		if (conn != null) {
			node.closeAsyncConnection(conn, eventLoop.index);
			conn = null;
		}
		else {
			node.decrAsyncConnection(eventLoop.index);
		}
	}

	private static final class InboundHandler extends ChannelInboundHandlerAdapter {
		private final NettyConnector connector;

		public InboundHandler(NettyConnector connector) {
			this.connector = connector;
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) {
			// Mark connection ready in regular (non TLS) mode.
			// Otherwise, wait for TLS handshake to complete.
			if (connector.state == AsyncCommand.CONNECT) {
				connector.channelActive();
			}
		}

		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) {
			connector.read((ByteBuf)msg);
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

			TlsPolicy tlsPolicy = connector.cluster.tlsPolicy;
			String tlsName = connector.node.getHost().tlsName;
			SSLSession session = ((SslHandler)ctx.pipeline().first()).engine().getSession();
			X509Certificate cert = (X509Certificate)session.getPeerCertificates()[0];

			Connection.validateServerCertificate(tlsPolicy, tlsName, cert);

			if (connector.state == AsyncCommand.TLS_HANDSHAKE) {
				connector.channelActive();
			}
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			if (connector == null) {
				Log.error("NettyConnector exception: " + Util.getStackTrace(cause));
				return;
			}
			connector.fail(new AerospikeException(cause));
		}
	}
}
