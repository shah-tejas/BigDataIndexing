package com.tejas.bdidemo.controller;

import com.tejas.bdidemo.exception.BadRequestException;
import com.tejas.bdidemo.exception.ResourceNotFoundException;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


@RestController
@RequestMapping("/plan")
public class PlanController {

    @ResponseStatus(value = HttpStatus.CREATED)
    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public String savePlan(@RequestBody String json) {

        JSONObject plan = new JSONObject(json);

        // Validate the plan against the schema
        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));

        Schema schema = SchemaLoader.load(plan_schema);
        try {
            schema.validate(plan);
        } catch (ValidationException e) {
            throw new BadRequestException(e.getMessage());
        }

        JedisPool jedisPool = new JedisPool();
        Jedis jedis = jedisPool.getResource();
        jedis.set((String) plan.get("objectId"), plan.toString());
        jedis.close();
        return "";
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{objectId}")
    public String getPlan(@PathVariable String objectId) {
        JedisPool jedisPool = new JedisPool();
        Jedis jedis = jedisPool.getResource();
        String plan = (String)jedis.get(objectId);
        if (plan == null || plan.isEmpty()) {
            throw new ResourceNotFoundException("Plan not found");
        }
        return plan;
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{objectId}")
    public String deletePlan(@PathVariable String objectId) {
        JedisPool jedisPool = new JedisPool();
        Jedis jedis = jedisPool.getResource();
        if (jedis.del(objectId) < 1) {
            throw new ResourceNotFoundException("Plan not found");
        }
        return "";
    }
}
