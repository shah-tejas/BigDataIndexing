package com.tejas.bdidemo.controller;

import com.tejas.bdidemo.BdiDemoApplication;
import com.tejas.bdidemo.exception.BadRequestException;
import com.tejas.bdidemo.exception.NotAuthorisedException;
import com.tejas.bdidemo.exception.PreConditionFailedException;
import com.tejas.bdidemo.exception.ResourceNotFoundException;
import com.tejas.bdidemo.security.JWTToken;
import com.tejas.bdidemo.service.PlanService;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
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
public class PlanController {

    static int count = 0;

    @Autowired
    private RabbitTemplate template;

    PlanService planService = new PlanService();

    Map<String, String> cacheMap = new HashMap<String, String>();

    JWTToken jwtToken = new JWTToken();

//    @RequestMapping(method = RequestMethod.GET, value = "/test")
//    public String index() {
//        PlanController.count++;
//
//        Map<String, String> actionMap = new HashMap<>();
//        actionMap.put("operation", "get");
//        actionMap.put("uri", "http://localhost:9200/testindex/_doc/" + PlanController.count);
//        String body = "{\"message\": \"test\", \"value\": 0 }";
//        actionMap.put("body", body);
//
//        System.out.println("Sending message: " + actionMap);
//
//        template.convertAndSend(BdiDemoApplication.MESSAGE_QUEUE, actionMap);
//
//        return Integer.toString(PlanController.count);
//    }

    @ResponseStatus(value = HttpStatus.CREATED)
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/token")
    public String generateToken() {
        String token;
        try {
            token = jwtToken.generateToken();
        } catch (Exception e) {
            throw new BadRequestException(e.getMessage());
        }

        return "{\"token\": \"" + token + "\"}";
    }

    @ResponseStatus(value = HttpStatus.CREATED)
    @RequestMapping(method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan")
    public String savePlan(@RequestBody String json, HttpServletRequest request, HttpServletResponse response) {

        // Authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

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

        // index object
        Map<String, String> actionMap = new HashMap<>();
        actionMap.put("operation", "SAVE");
        actionMap.put("uri", "http://localhost:9200");
        actionMap.put("index", "planindex");
        actionMap.put("body", json);

        System.out.println("Sending message: " + actionMap);

        template.convertAndSend(BdiDemoApplication.MESSAGE_QUEUE, actionMap);

        // cache the objectId
        this.cacheMap.put(objectKey, String.valueOf(plan.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(plan.hashCode()));

        return "{\"objectId\": \"" + objectKey + "\"}";
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> getPlan(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        // Authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

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

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.PUT, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> updatePlan(@RequestBody String json, @PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {
        // Authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

        //check etag
        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        }

        JSONObject plan = new JSONObject(json);

        //validate resource
        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));
        Schema schema = SchemaLoader.load(plan_schema);
        try {
            schema.validate(plan);
        } catch (ValidationException e) {
            throw new BadRequestException(e.getMessage());
        }

        //update resource
        String objectKey = this.planService.updatePlan(plan, (String)plan.get("objectType"));
        if (objectKey == null) {
            throw new BadRequestException("Update failed!");
        }

        // index object
        Map<String, String> actionMap = new HashMap<>();
        actionMap.put("operation", "SAVE");
        actionMap.put("uri", "http://localhost:9200");
        actionMap.put("index", "planindex");
        actionMap.put("body", json);

        System.out.println("Sending message: " + actionMap);

        template.convertAndSend(BdiDemoApplication.MESSAGE_QUEUE, actionMap);

        //update etag
        this.cacheMap.put(objectKey, String.valueOf(plan.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(plan.hashCode()));

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ResponseStatus(value = HttpStatus.OK)
    @RequestMapping(method = RequestMethod.PATCH, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> patchPlan(@RequestBody String json, @PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        //authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

        //check etag
        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        }

        JSONObject plan = new JSONObject(json);

        //merge json with saved value
        JSONObject mergedJson = this.planService.mergeJson(plan, objectId);
        if (mergedJson == null) {
            throw new ResourceNotFoundException("Resource not found");
        }

        //validate json
        JSONObject plan_schema = new JSONObject(new JSONTokener(PlanController.class.getResourceAsStream("/plan-schema.json")));
        Schema schema = SchemaLoader.load(plan_schema);
        try {
            schema.validate(mergedJson);
        } catch (ValidationException e) {
            throw new BadRequestException(e.getMessage());
        }

        JSONObject planToIndex = new JSONObject(mergedJson.toString());

        //update json
        String objectKey = this.planService.updatePlan(mergedJson, (String)mergedJson.get("objectType"));
        if (objectKey == null) {
            throw new BadRequestException("Update failed!");
        }

        // index object
        Map<String, String> actionMap = new HashMap<>();
        actionMap.put("operation", "SAVE");
        actionMap.put("uri", "http://localhost:9200");
        actionMap.put("index", "planindex");
        actionMap.put("body", planToIndex.toString());

        System.out.println("Sending message: " + actionMap);

        template.convertAndSend(BdiDemoApplication.MESSAGE_QUEUE, actionMap);

        //update etag
        this.cacheMap.put(objectKey, String.valueOf(mergedJson.hashCode()));
        response.setHeader(HttpHeaders.ETAG, String.valueOf(mergedJson.hashCode()));

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @ResponseStatus(value = HttpStatus.NO_CONTENT)
    @RequestMapping(method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE, value = "/plan/{objectId}")
    public ResponseEntity<String> deletePlan(@PathVariable String objectId, HttpServletRequest request, HttpServletResponse response) {

        // Authorization
        String token = request.getHeader("Authorization").replace("Bearer ", "");
        if (!this.jwtToken.isTokenValid(token)){
            throw new NotAuthorisedException("The token is not valid");
        }

        String if_match = request.getHeader(HttpHeaders.IF_MATCH);
        if (if_match == null || if_match.isEmpty()) {
            // etag not provided throw 404
            throw new ResourceNotFoundException("etag not provided in request");
        }
        if (this.cacheMap.get(objectId) != null && !this.cacheMap.get(objectId).equals(if_match)) {
            // hash found in cache but does not match with etag
            throw new PreConditionFailedException("etag in request does not match hash in cache");
        } else {
            if (!this.planService.deletePlan(objectId)) {
                // deletion failed
                throw new ResourceNotFoundException("Plan not found");
            }

            String indexObjectId = objectId.split("_")[1];

            // index object
            Map<String, String> actionMap = new HashMap<>();
            actionMap.put("operation", "DELETE");
            actionMap.put("uri", "http://localhost:9200");
            actionMap.put("index", "planindex");
            actionMap.put("body", "{ \"objectId\" : \"" + indexObjectId + "\" }");

            System.out.println("Sending message: " + actionMap);

            template.convertAndSend(BdiDemoApplication.MESSAGE_QUEUE, actionMap);

            //delete the cache
            this.cacheMap.remove(objectId);
            return new ResponseEntity<>(HttpStatus.NO_CONTENT);
        }
    }

}
