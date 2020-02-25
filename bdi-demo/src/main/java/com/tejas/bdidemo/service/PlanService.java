package com.tejas.bdidemo.service;

import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class PlanService {

    private JedisPool jedisPool;

    private JedisPool getJedisPool() {
        if (this.jedisPool == null) {
            return new JedisPool();
        }
        return this.jedisPool;
    }

    public String savePlan(JSONObject json, String objectType) {
        // temp array of keys to remove from json object
        ArrayList<String> keysToDelete = new ArrayList<String>();

        // Iterate through the json
        for(String key : json.keySet()) {
            // check if the value of key is JSONObject or JSONArray
            // first get current Value
            Object currentValue = json.get(key);
            if(currentValue instanceof JSONObject) {
                String objectKey = this.savePlan((JSONObject)currentValue, key);
                // remove this value from JSON, as it will be stored separately
                keysToDelete.add(key);

                // save the relation as separate key in redis
                Jedis jedis = this.getJedisPool().getResource();
                String relationKey = objectType + "_" + json.get("objectId") + "_" + key;
                jedis.set(relationKey, objectKey);
                jedis.close();

            } else if (currentValue instanceof JSONArray) {
                JSONArray currentArrayValue = (JSONArray)currentValue;
                //temp array to store keys of individual objects
                String[] tempValues = new String[currentArrayValue.length()];

                //iterate through the array
                for (int i = 0; i < currentArrayValue.length(); i++) {
                    if (currentArrayValue.get(i) instanceof JSONObject) {
                        JSONObject arrayObject = (JSONObject)currentArrayValue.get(i);
                        String arrayObjectKey = this.savePlan(arrayObject, (String)arrayObject.get("objectType"));

                        tempValues[i] = arrayObjectKey;
                    }
                }

                keysToDelete.add(key);

                // save the Array as separate key in redis
                Jedis jedis = this.getJedisPool().getResource();
                String relationKey = objectType + "_" + json.get("objectId") + "_" + key;
                jedis.set(relationKey, Arrays.toString(tempValues));
                jedis.close();

            }
        }

        // Remove objects from json that are stored separately
        for (String key : keysToDelete) {
            json.remove(key);
        }

        //save the current object in redis
        String objectKey = objectType + "_" + json.get("objectId");

        Jedis jedis = this.getJedisPool().getResource();
        jedis.set(objectKey, json.toString());
        jedis.close();

        return objectKey;
    }

    public JSONObject getPlan(String planKey) {
        JedisPool jedisPool = new JedisPool();
        Jedis jedis;
        JSONObject json;
        if (isStringArray(planKey)) {
            String arrayValue = getFromArrayString(planKey);
            json = new JSONObject(arrayValue);
        } else {
            jedis = jedisPool.getResource();
            String jsonString = jedis.get(planKey);
            jedis.close();
            if (jsonString == null || jsonString.isEmpty()) {
                return null;
            }
            System.out.println(jsonString);
            json = new JSONObject(jsonString);
        }

        // fetch additional relations for the object, if present
        jedis = jedisPool.getResource();
        Set<String> jsonRelatedKeys = jedis.keys(planKey + "_*");
        jedis.close();

        Iterator<String> keysIterator = jsonRelatedKeys.iterator();
        while(keysIterator.hasNext()) {
            String partObjKey = keysIterator.next();
            String partObjectKey = partObjKey.substring(partObjKey.lastIndexOf('_')+1);

            // fetch the id stored at partObjKey
            jedis = jedisPool.getResource();
            String partObjectDBKey = jedis.get(partObjKey);
            jedis.close();
            if (partObjectDBKey == null || partObjectDBKey.isEmpty()) {
                continue;
            }

            JSONObject partObj = this.getPlan(partObjectDBKey);
            //add partObj to original object
            json.put(partObjectKey, partObj);
        }

        return json;
    }

    private boolean isStringArray(String str) {
        if (str.indexOf('[') < str.indexOf(']')) {
            if (str.substring((str.indexOf('[') + 1), str.indexOf(']')).split(", ").length > 0)
                return true;
            else
                return false;
        } else {
            return false;
        }
    }

    private String getFromArrayString(String keyArray) {
        ArrayList<String> jsonArray = new ArrayList<>();
        String[] array = keyArray.substring((keyArray.indexOf('[') + 1), keyArray.indexOf(']')).split(", ");

        for (String key : array) {
            JSONObject partObj = this.getPlan(key);
            jsonArray.add(partObj.toString());
        }

        return jsonArray.toString();
    }
}