package com.demo.sparkstream;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @Author: 君墨笑
 * @Date: 2025/9/17 19:47
 */
public class ReceiverSocket {

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(9666);
        Socket clientSocket = serverSocket.accept();
        if (clientSocket.isConnected()) {
            System.out.println("获取到了客户端连接");
            new Thread(() -> {
                try {
                    OutputStream outputStream = clientSocket.getOutputStream();
                    PrintStream printStream = new PrintStream(outputStream);
                    while (true) {
                        System.out.println("打印数据");
                        printStream.println("Hello Word");
                        printStream.println("Java Demo");
                        Thread.sleep(2000l);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }

    }
}