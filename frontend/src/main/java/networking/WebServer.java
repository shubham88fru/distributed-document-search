package networking;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String STATUS_ENDPOINT = "/status";
    private static final String HOME_PAGE_ENDPOINT = "/";
    private static final String HOME_PAGE_UI_ASSETS_BASE_DIR = "/ui_assets/";

    private final int port;
    private HttpServer server;
    private final OnRequestCallback  onRequestCallback;

    public WebServer(int port, OnRequestCallback onRequestCallback) {
        this.port = port;
        this.onRequestCallback = onRequestCallback;
    }

    public void startServer() {
        //create an http server.
        try {
            //size of backlog is the number of requests we allow the server to keep
            //in queue, if there aren't enough threads to take the request.
            this.server = HttpServer.create(new InetSocketAddress(port), 0);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        HttpContext statusContext = server.createContext(STATUS_ENDPOINT); // `/status` endpoint.
        statusContext.setHandler(this::handleStatusCheckRequest); //handler for /status endpoint.

        HttpContext taskContext = server.createContext(onRequestCallback.getEndpoint()); // `/task` endpoint.
        taskContext.setHandler(this::handleTaskRequest); //handler for /task endpoint.

        HttpContext homePageContext = server.createContext(HOME_PAGE_ENDPOINT);
        homePageContext.setHandler(this::handleRequestForAsset);

        server.setExecutor(Executors.newFixedThreadPool(8)); //thread pool for the server.
        server.start();
    }

    private void handleRequestForAsset(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }

        byte[] response;
        String asset = exchange.getRequestURI().getPath();
        if (asset.equals(HOME_PAGE_ENDPOINT)) {
            response = readUIAsset(HOME_PAGE_UI_ASSETS_BASE_DIR + "index.html");
        } else {
            response = readUIAsset(asset);
        }

        addContentType(asset, exchange);
        sendResponse(response, exchange);
    }

    private byte[] readUIAsset(String asset) throws IOException {
        InputStream assetStream = getClass().getResourceAsStream(asset);
        if (assetStream == null) {
            return new byte[]{};
        }
        return assetStream.readAllBytes();
    }

    private static void addContentType(String asset, HttpExchange exchange) throws IOException {
        String contentType = "text/html";
        if (asset.endsWith("js")) {
            contentType = "text/javascript";
        } else if (asset.endsWith("css")) {
            contentType = "text/css";
        }
        exchange.getResponseHeaders().add("Content-Type", contentType);
    }

    //methods to handle the requests to endpoints.
    // /status
    private void handleStatusCheckRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("get")) {
            exchange.close();
            return;
        }

        String responseMessage = "Server is alive";
        sendResponse(responseMessage.getBytes(), exchange);
    }

    private void handleTaskRequest(HttpExchange exchange) throws IOException {
        if (!exchange.getRequestMethod().equalsIgnoreCase("post")) {
            exchange.close();
            return;
        }

        byte[] responseBytes = onRequestCallback.handleRequest(exchange.getRequestBody().readAllBytes());
        sendResponse(responseBytes, exchange);
    }

    private byte[] calculateResponse(byte[] requestBytes) {
        String bodyString = new String(requestBytes);
        String[] stringNumbers = bodyString.split(",");
        BigInteger result = BigInteger.ONE;
        for (String number: stringNumbers) {
            BigInteger bigInteger = new BigInteger(number);
            result = result.multiply(bigInteger);
        }

        return String.format("Result of multiplication is %s\n", result).getBytes();
    }

    private void sendResponse(byte[] responseBytes, HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, responseBytes.length);
        OutputStream outputStream = exchange.getResponseBody();
        outputStream.write(responseBytes);
        outputStream.flush();
        outputStream.close();
    }

    public void stop() {
        server.stop(10);
    }
}