package com.tejas.bdidemo.listener;

import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

@Component
public class IndexingListener {

    public void receiveMessage(Map<String, String> message) {
        System.out.println("Message received: " + message);
        String operation = message.get("operation");
        String uri = message.get("uri");
        String body = message.get("body");

        HttpPost post = new HttpPost(uri);

        String result = "";

        try {
            post.setEntity(new StringEntity(body));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        post.addHeader(HttpHeaders.CONTENT_TYPE, String.valueOf(ContentType.APPLICATION_JSON));

        try(CloseableHttpClient httpClient = HttpClients.createDefault();
            CloseableHttpResponse response = httpClient.execute(post)) {
            result = EntityUtils.toString(response.getEntity());
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Result: " + result);
    }

}
