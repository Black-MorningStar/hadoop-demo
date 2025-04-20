package com.demo;

import org.apache.hadoop.fs.Path;

import java.net.URI;

public class Main {
    public static void main(String[] args) {
        Path path = new Path("/data/result/demo.txt");
        System.out.println(path.getName());
        System.out.println(path.toString());
        URI uri = path.toUri();
        System.out.println(uri.getPath());
        System.out.println(uri.toString());
        System.out.println("Hello world!");
    }
}