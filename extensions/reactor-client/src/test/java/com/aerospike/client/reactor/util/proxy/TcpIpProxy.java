/*
 * Copyright 2012-2018 Aerospike, Inc.
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
package com.aerospike.client.reactor.util.proxy;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

public class TcpIpProxy {

    private final String remoteHost;
    private final int remotePort;
    private final int localPort;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile long delayOnResponse = 0;

    public TcpIpProxy(String remoteHost, int remotePort) {
        this(remoteHost, remotePort, 0);
    }

    public TcpIpProxy(String remoteHost, int remotePort, int localPort) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.localPort = localPort;
    }

    public void setDelayOnResponse(long delayOnResponse) {
        this.delayOnResponse = delayOnResponse;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(localPort);
        acceptThread = new Thread(() -> {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    new Thread(new Connection(socket, remoteHost, remotePort, delayOnResponse)).start();
                } catch (SocketException e) {
                    if(serverSocket.isClosed()){
                        break;
                    } else {
                        throw new RuntimeException(e);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        acceptThread.start();
    }

    public int getLocalPort() {
        return serverSocket.getLocalPort();
    }

    public String getLocalAddress() {
        return serverSocket.getInetAddress().getHostName();
    }

    public void close() throws Exception {
        serverSocket.close();
    }
}