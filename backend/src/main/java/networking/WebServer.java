package networking;

import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class WebServer {
    private static final String TASK_ENDPOINT = "/task";
    private static final String STATUS_ENDPOINT = "/status";

    private final int port;
    private HttpServer server;
    private final OnRequestCallback  onRequestCallback;

    public WebServer(int port, OnRequestCallback onRequestCallback) {
        this.port = port;
        this.onRequestCallback = onRequestCallback;
    }

//    public static void main(String[] args) {
//        int serverPort = 8080;
//        if (args.length == 1) {
//            serverPort = Integer.parseInt(args[0]);
//        }
//
//        WebServer webServer = new WebServer(serverPort);
//        webServer.startServer();
//        System.out.println("Server is listening of port " + serverPort);
//    }

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

        server.setExecutor(Executors.newFixedThreadPool(8)); //thread pool for the server.
        server.start();
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
}