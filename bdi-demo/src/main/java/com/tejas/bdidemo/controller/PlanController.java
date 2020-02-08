package com.tejas.bdidemo.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@RestController
@RequestMapping("/plan")
public class PlanController {

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity savePlan(@RequestBody String json) {
        Gson gson = new Gson();
        JsonObject plan = gson.fromJson(json, JsonObject.class);
        JedisPool jedisPool = new JedisPool();
        Jedis jedis = jedisPool.getResource();
        String objectId = plan.get("objectId").getAsString();
        jedis.set("Plan:" + objectId, plan.toString());
        jedis.close();
        return new ResponseEntity(HttpStatus.CREATED);
    }
}
