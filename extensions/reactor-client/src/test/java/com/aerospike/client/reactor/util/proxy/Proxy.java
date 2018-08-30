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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

public class Proxy implements Runnable {

    private final Socket in;
    private final Socket out;
    private long delayOnWriteInMs;

    Proxy(Socket in, Socket out, long delayOnWriteInMs) {
        this.in = in;
        this.out = out;
        this.delayOnWriteInMs = delayOnWriteInMs;
    }

    @Override
    public void run() {
        try {
            runThrowing();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void runThrowing() throws IOException {
        try {
            InputStream inputStream = in.getInputStream();
            OutputStream outputStream = out.getOutputStream();

            byte[] reply = new byte[4096];
            int bytesRead;
            while (-1 != (bytesRead = inputStream.read(reply))) {
                if(delayOnWriteInMs > 0){
                    try {
                        Thread.sleep(delayOnWriteInMs);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                outputStream.write(reply, 0, bytesRead);
                outputStream.flush();
            }
        }
        catch (SocketException e) {
            if(!in.isClosed() && !out.isClosed()){
                throw new RuntimeException(e);
            }
        }
        finally {
            in.close();
        }
    }
}