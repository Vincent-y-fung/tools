package org.example;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.proxy.AsyncProxyServlet;

public class JettyReverseProxy {
    public static void main(String[] args) throws Exception {
        // 创建 Jetty 服务器，监听 8080 端口
        Server server = new Server(8080);

        // 创建 Servlet 上下文处理器
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        // 配置反向代理 Servlet
        ServletHolder proxyServlet = new ServletHolder(new AsyncProxyServlet.Transparent());
        proxyServlet.setInitParameter("targetUri", "http://example.com"); // 目标服务器地址
        proxyServlet.setInitParameter("prefix", "/"); // 代理路径前缀（可选）
        context.addServlet(proxyServlet, "/*"); // 所有请求都代理到目标服务器

        // 启动服务器
        server.start();
        server.join();
    }
}
