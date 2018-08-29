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