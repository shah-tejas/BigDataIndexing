package com.tejas.bdidemo.controller;

import com.tejas.bdidemo.exception.BadRequestException;
import com.tejas.bdidemo.exception.PreConditionFailedException;
import com.tejas.bdidemo.exception.ResourceNotFoundException;
import com.tejas.bdidemo.service.PlanService;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;


@RestController
@RequestMapping("/plan")
public class PlanController {

    PlanService planService = new PlanService();

    Map<String, String> cacheMap = new HashMap<String, String>();

    @ResponseStatus(value = HttpStatus.CREATED)
    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public String savePlan(@RequestBody String json, HttpServletResponse response) {

        JSONObject plan = new JSONObject(json);

        // Validate the plan against the schema
        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));

        Schema schema = SchemaLoader.load(plan_schema);
        try {
            schema.validate(plan);
        } catch (ValidationException e) {
            throw new BadRequestException(e.getMessage());
        }

        String objectKey = this.planService.savePlan(plan, (String)plan.get("objectType"));

        // cache the objectId
        this.cacheMap.put(objectKey, String.valueOf(plan.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(plan.hashCode()));

        return "{\"objectId\": \"" + objectKey + "\"}";
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{objectId}")
    public ResponseEntity<String> getPlan(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        String if_none_match = request.getHeader(HttpHeaders.IF_NONE_MATCH);
        if (this.cacheMap.get(objectId) != null && this.cacheMap.get(objectId).equals(if_none_match)) {
            // etag matches, send 304
            return new ResponseEntity<String>(HttpStatus.NOT_MODIFIED);
        }

        JedisPool jedisPool = new JedisPool();
        Jedis jedis = jedisPool.getResource();
        JSONObject json = this.planService.getPlan(objectId);
        if (json == null) {
            throw new ResourceNotFoundException("Plan not found");
        }

        // cache the objectId
        this.cacheMap.put(objectId, String.valueOf(json.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(json.hashCode()));

        return ResponseEntity.ok().body(json.toString());
    }

    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    @RequestMapping(method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/{objectId}")
    public ResponseEntity<String> deletePlan(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        } else {

            JedisPool jedisPool = new JedisPool();
            Jedis jedis = jedisPool.getResource();
            if (jedis.del(objectId) < 1) {
                jedis.close();
                throw new ResourceNotFoundException("Plan not found");
            }
            jedis.close();
            //delete the cache
            this.cacheMap.remove(objectId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
    }
}
