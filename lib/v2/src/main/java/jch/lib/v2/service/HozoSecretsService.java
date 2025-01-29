package jch.lib.v2.service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.json.JSONObject;

public abstract class HozoSecretsService {
    static final String BASE_URI = "https://zsm.hozo.com/api/secrets";
    static final String ALIVE_PART = "/heartbeat";
    static final String AUTH_PART = "/authenticate";
    static final String GET_PART = "/get/";

    public static boolean isAlive() {
        try {
            //System.out.println(BASE_URI + ALIVE_PART);
            URL request = new URL(BASE_URI + ALIVE_PART);
            HttpURLConnection con = (HttpURLConnection) request.openConnection();
            con.setRequestMethod("GET");

            InputStream response = con.getInputStream();
            String out = convertInputStreamToString(response);
            JSONObject json = new JSONObject(out);

            if(json.get("alive") != null) {
                return true;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static String authenticate(String userName, String password) {
        HashMap<String, String> root = new HashMap<>();
        root.put("password", password);
        root.put("username", userName);
        JSONObject jsonObj = new JSONObject(root);
        //System.out.println(jsonObj);

        try {
            URL request = new URL(BASE_URI + AUTH_PART);
            HttpURLConnection con = (HttpURLConnection) request.openConnection();
            con.setRequestMethod("POST");
            con.setReadTimeout(15*1000);  //15 seconds
            con.setDoOutput(true);
            con.setRequestProperty("content-Type", "application/json");

            try(OutputStream os = con.getOutputStream()) {
                byte[] input = jsonObj.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            InputStream response = con.getInputStream();
            String out = convertInputStreamToString(response);
            JSONObject json = new JSONObject(out);
            return json.getString("access_token");

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static JSONObject getSecret(String token, String secretId) {
        try {
            URL request = new URL(BASE_URI + GET_PART + secretId);
            HttpURLConnection con = (HttpURLConnection) request.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("authorization", "Bearer " + token);

            InputStream response = con.getInputStream();
            String out = convertInputStreamToString(response);
            return new JSONObject(out);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static String convertInputStreamToString(InputStream is) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        int length;
        while ((length = is.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        return result.toString(StandardCharsets.UTF_8);
    }
}
