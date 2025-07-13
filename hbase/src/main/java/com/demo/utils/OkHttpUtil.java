package com.demo.utils;

import okhttp3.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class OkHttpUtil {

    // 单例 OkHttpClient 实例，全局复用
    private static final OkHttpClient client;

    static {
        client = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)   // 连接超时
                .readTimeout(15, TimeUnit.SECONDS)      // 读取超时
                .writeTimeout(10, TimeUnit.SECONDS)     // 写超时
                .retryOnConnectionFailure(true)         // 自动重试
                .build();
    }

    // ---------------------- GET ----------------------
    public static String get(String url, Map<String, String> headers) throws IOException {
        Request.Builder builder = new Request.Builder().url(url);
        if (headers != null) {
            headers.forEach(builder::addHeader);
        }

        Request request = builder.get().build();
        try (Response response = client.newCall(request).execute()) {
            return handleResponse(response);
        }
    }

    // ---------------------- POST JSON ----------------------
    public static String postJson(String url, String jsonBody, Map<String, String> headers) throws IOException {
        RequestBody body = RequestBody.create(jsonBody, MediaType.parse("application/json"));

        Request.Builder builder = new Request.Builder().url(url).post(body);
        if (headers != null) {
            headers.forEach(builder::addHeader);
        }

        Request request = builder.build();
        try (Response response = client.newCall(request).execute()) {
            return handleResponse(response);
        }
    }

    // ---------------------- POST FORM ----------------------
    public static String postForm(String url, Map<String, String> formData) throws IOException {
        FormBody.Builder formBuilder = new FormBody.Builder();
        if (formData != null) {
            formData.forEach(formBuilder::add);
        }

        Request request = new Request.Builder()
                .url(url)
                .post(formBuilder.build())
                .build();

        try (Response response = client.newCall(request).execute()) {
            return handleResponse(response);
        }
    }

    // ---------------------- 响应处理 ----------------------
    private static String handleResponse(Response response) throws IOException {
        if (!response.isSuccessful()) {
            throw new IOException("HTTP 请求失败，状态码：" + response.code());
        }
        return response.body() != null ? response.body().string() : null;
    }
}
