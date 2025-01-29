package jch.lib.v2.service;

import org.json.JSONObject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

public class HozoSecretsServiceTest {
    static String username = "";
    static String password = "";
    static String secretId = "";

    public static String HOME =
            System.getenv("HOME") != null ?
                    System.getenv("HOME") : System.getenv("UserProfile");

    static {
        String p = HOME + "\\Keys\\hozo\\testuser.json";
        //System.out.println(p);
        Path path = Paths.get(p);
        try {
            String creds = Files.readAllLines(path).get(0);
            //System.out.println(creds);
            JSONObject jsonCreds = new JSONObject(creds);
            username = jsonCreds.getString("username");
            password = jsonCreds.getString("password");
            secretId = jsonCreds.getString("secretId");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args){
        String token = HozoSecretsService.authenticate(username, password);
        JSONObject jsonSecret = HozoSecretsService.getSecret(token,secretId);
        assert jsonSecret != null;
        System.out.println(jsonSecret.getString("username"));
    }

    @Test
    void isAlive() {
        final boolean alive = HozoSecretsService.isAlive();
        assert alive;
    }

    @Test
    void isAuthenticated() {
        assert HozoSecretsService.authenticate(username, password) != null;
    }

    //@Test
    void getSecret() {
        String token = HozoSecretsService.authenticate(username, password);
        JSONObject jsonSecret = HozoSecretsService.getSecret(token,"");
        assert jsonSecret != null;

        String username = jsonSecret.getString("username");
        assert username != null;
    }
}
