package org.example;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HighPerfReverseProxy {
    private static final Logger logger = Logger.getLogger(HighPerfReverseProxy.class.getName());
    private final int proxyPort;
    private final String targetHost;
    private final int targetPort;
    private final Server jettyServer;
    private final HttpClient httpClient;
    private final ExecutorService workerPool;

    public HighPerfReverseProxy(int proxyPort, String targetHost, int targetPort) {
        this.proxyPort = proxyPort;
        this.targetHost = targetHost;
        this.targetPort = targetPort;

        // 1. 优化Jetty线程池（核心优化点）
        QueuedThreadPool jettyThreadPool = new QueuedThreadPool(
                Runtime.getRuntime().availableProcessors() * 4,  // 最大线程数（CPU*4）
                Runtime.getRuntime().availableProcessors() * 2,  // 核心线程数（CPU*2）
                30, //TimeUnit.SECONDS,                            // 空闲线程存活时间
                new java.util.concurrent.LinkedBlockingQueue<>(1000)  // 任务队列大小
        );
        jettyThreadPool.setName("jetty-http");

        // 2. 初始化Jetty服务器
        this.jettyServer = new Server(jettyThreadPool);
        ServerConnector connector = new ServerConnector(jettyServer,
                new HttpConnectionFactory(new HttpConfiguration()));
        connector.setPort(proxyPort);
        connector.setIdleTimeout(Duration.ofSeconds(30).toMillis());  // 连接空闲超时
        jettyServer.addConnector(connector);

        // 3. 配置业务处理线程池（独立于Jetty IO线程）
        this.workerPool = new ThreadPoolExecutor(
                Runtime.getRuntime().availableProcessors() * 2,
                Runtime.getRuntime().availableProcessors() * 4,
                30, TimeUnit.SECONDS,
                new java.util.concurrent.SynchronousQueue<>(),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

        // 4. 初始化高性能HTTP客户端（复用之前的优化）
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .followRedirects(HttpClient.Redirect.NORMAL)
                .connectTimeout(Duration.ofSeconds(5))
                .executor(workerPool)
                .build();

        // 5. 注册反向代理处理器
        jettyServer.setHandler(new ProxyHandler());
    }

    public void start() throws Exception {
        jettyServer.start();
        logger.info(String.format("Jetty Reverse Proxy started on port %d, forwarding to %s:%d",
                proxyPort, targetHost, targetPort));
    }

    public void stop() throws Exception {
        jettyServer.stop();
        workerPool.shutdownNow();
        logger.info("Jetty Reverse Proxy stopped");
    }

    private class ProxyHandler extends AbstractHandler {
        @Override
        public void handle(String target, Request baseRequest,
                           HttpServletRequest request, HttpServletResponse response) throws IOException {
            baseRequest.setHandled(true);  // 标记请求已处理

            // 开启异步处理（Jetty关键优化）
            AsyncContext async = request.startAsync();
            async.setTimeout(Duration.ofSeconds(30).toMillis());

            workerPool.execute(() -> {
                try (InputStream reqBody = request.getInputStream();
                     OutputStream respBody = response.getOutputStream()) {

                    // 构建目标URL
                    String targetUrl = "http://" + targetHost + ":" + targetPort +
                            request.getRequestURI() + (request.getQueryString() != null ?
                            "?" + request.getQueryString() : "");

                    // 构建HTTP请求
                    HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                            .uri(URI.create(targetUrl))
                            .timeout(Duration.ofSeconds(15))
                            .method(request.getMethod(),
                                    HttpRequest.BodyPublishers.ofInputStream(() -> reqBody));

                    // 复制有效请求头
                    request.getHeaderNames().asIterator().forEachRemaining(header -> {
                        if (!isConnectionHeader(header)) {
                            reqBuilder.header(header, request.getHeader(header));
                        }
                    });

                    // 异步发送请求并流式处理响应
                    HttpResponse<InputStream> proxyResponse = httpClient.send(
                            reqBuilder.build(),
                            HttpResponse.BodyHandlers.ofInputStream()
                    );

                    // 设置响应状态和头信息
                    response.setStatus(proxyResponse.statusCode());
                    proxyResponse.headers().map().forEach((k, v) ->
                            v.forEach(val -> response.addHeader(k, val)));

                    // 流式复制响应内容（关键性能点）
                    proxyResponse.body().transferTo(respBody);

                    logger.info(String.format("Processed %s %s -> %d (ActiveThreads: %d)",
                            request.getMethod(),
                            request.getRequestURI(),
                            proxyResponse.statusCode(),
                            ((QueuedThreadPool) jettyServer.getThreadPool()).getThreads()));

                } catch (Exception e) {
                    response.setStatus(500);
                    logger.log(Level.SEVERE, "Proxy error: " + request.getRequestURI(), e);
                } finally {
                    async.complete();  // 完成异步处理
                }
            });
        }

        private boolean isConnectionHeader(String key) {
            return key.equalsIgnoreCase("Connection") ||
                    key.equalsIgnoreCase("Keep-Alive") ||
                    key.equalsIgnoreCase("Proxy-Connection") ||
                    key.equalsIgnoreCase("Transfer-Encoding");
        }
    }

    public static void main(String[] args) {
        try {
            int proxyPort = 8080;
            String targetHost = "backend-service";  // 替换为实际目标主机
            int targetPort = 8081;                  // 替换为实际目标端口

            HighPerfReverseProxy proxy = new HighPerfReverseProxy(proxyPort, targetHost, targetPort);
            proxy.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    proxy.stop();
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Failed to stop proxy", e);
                }
            }));

            System.out.println("Jetty Reverse Proxy running...");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to start proxy", e);
        }
    }
}
