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