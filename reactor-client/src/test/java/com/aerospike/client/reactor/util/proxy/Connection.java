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