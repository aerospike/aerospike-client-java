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
import java.net.Socket;

public class Connection implements Runnable {

    private final Socket clientSocket;
    private final String remoteHost;
    private final int remotePort;
    private long delayOnReadInMs;

    Connection(Socket clientSocket, String remoteHost, int remotePort, long delayOnResponseInMs) {
        this.clientSocket = clientSocket;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.delayOnReadInMs = delayOnResponseInMs;
    }

    @Override
    public void run() {
        Socket serverSocket;
        try {
            serverSocket = new Socket(remoteHost, remotePort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        new Thread(new Proxy(clientSocket, serverSocket, 0)).start();
        new Thread(new Proxy(serverSocket, clientSocket, delayOnReadInMs)).start();
        new Thread(() -> {
            while (true) {
                if (clientSocket.isClosed()) {
                    if (!serverSocket.isClosed()) {
                        try {
                            serverSocket.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    break;
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {}
            }
        }).start();
    }

}