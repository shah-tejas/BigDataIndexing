package com.tejas.bdidemo.listener;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
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
        String indexName = message.get("index");

        JSONObject jsonBody = new JSONObject(body);

        switch (operation) {
            case "SAVE": {
                if (!checkIndexMapping(uri, indexName, jsonBody)) {
                    this.createIndexMapping(uri, indexName, jsonBody);
                }
                this.indexObject(uri, indexName, jsonBody);
                break;
            }
            case "DELETE": {
                this.deleteIndex(uri, indexName, jsonBody);
                break;
            }
        }
    }

    private int executeRequest(HttpUriRequest request) {
        int result = 0;
        try(CloseableHttpClient httpClient = HttpClients.createDefault();
            CloseableHttpResponse response = httpClient.execute(request)) {
            System.out.println("ElasticSearch Response: " + response.toString());
            result = response.getStatusLine().getStatusCode();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private boolean checkIndexMapping(String uri, String indexName, JSONObject objectBody) {
        // check if mapping is present
        HttpUriRequest request = new HttpGet(uri + "/" + indexName + "/_mapping");
        if(executeRequest(request) == HttpStatus.SC_NOT_FOUND) {
            return false;
        } else {
            return true;
        }
    }

    private JSONObject getMappingJSON(JSONObject object) {
        JSONObject obj = new JSONObject();

        for(String key : object.keySet()) {
            Object value = object.get(key);
            if(value instanceof JSONObject) {
                JSONObject nestedObj = new JSONObject();
                JSONObject nestedProperties = getMappingJSON((JSONObject)value);
                if(nestedProperties.length() > 0) {
                    nestedObj.put("properties", nestedProperties);
                }
                nestedObj.put("type", "nested");
                obj.put(key, nestedObj);
            } else if (value instanceof JSONArray) {
                JSONObject nestedObj = new JSONObject();
                JSONObject nestedProperties = getMappingJSON(((JSONArray)value).getJSONObject(0));
                if(nestedProperties.length() > 0) {
                    nestedObj.put("properties", nestedProperties);
                }
                nestedObj.put("type", "nested");
                obj.put(key, nestedObj);
            }
        }

        return obj;
    }

    private void createIndexMapping(String uri, String indexName, JSONObject objectBody) {
        JSONObject mappingObj = new JSONObject("{ \"mappings\": { \"properties\" : {} } }");
        JSONObject objProperties = this.getMappingJSON(objectBody);
        ((JSONObject)mappingObj.get("mappings")).put("properties", objProperties);
        System.out.println("Mapping Object: " + mappingObj.toString());

        HttpPut request = new HttpPut(uri + "/" + indexName);
        request.addHeader(HttpHeaders.CONTENT_TYPE, String.valueOf(ContentType.APPLICATION_JSON));
        try {
            request.setEntity(new StringEntity(mappingObj.toString()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        this.executeRequest(request);
    }

    private void indexObject(String uri, String indexName, JSONObject objectBody) {
        HttpPut request = new HttpPut(uri + "/" + indexName + "/_doc/" + objectBody.getString("objectId"));
        request.addHeader(HttpHeaders.CONTENT_TYPE, String.valueOf(ContentType.APPLICATION_JSON));
        try {
            request.setEntity(new StringEntity(objectBody.toString()));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        this.executeRequest(request);
    }

    private void deleteIndex(String uri, String indexName, JSONObject objectBody) {
        HttpDelete request = new HttpDelete(uri + "/" + indexName + "/_doc/" + objectBody.getString("objectId"));

        this.executeRequest(request);
    }

}
