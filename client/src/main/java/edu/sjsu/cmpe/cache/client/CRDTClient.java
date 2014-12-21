package edu.sjsu.cmpe.cache.client;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.util.concurrent.Future;
import com.mashape.unirest.http.async.Callback;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.lang.Integer;

/**
 * Distributed cache service
 * 
 */
public class CRDTClient implements CacheServiceInterface {

    public int successCount = 0;
    private boolean isServer1Complete = false;
    private boolean isServer2Complete = false;
    private boolean isServer3Complete = false;

    private HashMap<String, String> serverMessage;

    private String server1Message="";
    private String server2Message="";
    private String server3Message="";

    public boolean isUp1 = false;
    public boolean isUp2 = false;
    public boolean isUp3 = false;
    private CountDownLatch doneLatch;

    public CRDTClient() {
        doneLatch = new CountDownLatch(3);
        serverMessage = new HashMap<String, String>();
        serverMessage.put("serverA", "");
        serverMessage.put("serverB", "");
        serverMessage.put("serverC", "");
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#get(long)
     */
    @Override
    public String get(long key){
        doneLatch = new CountDownLatch(3);
        System.out.println("Getting key " + key);
            Future<HttpResponse<JsonNode>> server1 = Unirest.get("http://localhost:3000/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        public void failed(UnirestException e) {
                            System.out.println("Server error 3000");
                            doneLatch.countDown();
                        }

                        public void completed(HttpResponse<JsonNode> response) {
                            successCount++;
                            server1Message = response.getBody().getObject().getString("value").toString();
                            serverMessage.put("serverA", server1Message);
                            doneLatch.countDown();
                        }

                        public void cancelled() {
                            System.out.println("The request has been cancelled");
                            doneLatch.countDown();
                        }

                    });


            Future<HttpResponse<JsonNode>> server2 = Unirest.get("http://localhost:3001/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        public void failed(UnirestException e) {
                            System.out.println("Server error 3001");
                            doneLatch.countDown();
                        }

                        public void completed(HttpResponse<JsonNode> response) {
                            server2Message = response.getBody().getObject().getString("value").toString();
                            serverMessage.put("serverB", server2Message);
                            doneLatch.countDown();
                        }

                        public void cancelled() {
                            System.out.println("The request has been cancelled");
                            doneLatch.countDown();
                        }

                    });



            Future<HttpResponse<JsonNode>> server3 = Unirest.get("http://localhost:3002/cache/{key}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .asJsonAsync(new Callback<JsonNode>() {

                        public void failed(UnirestException e) {
                            System.out.println("Server error 3002");
                            doneLatch.countDown();
                        }

                        public void completed(HttpResponse<JsonNode> response) {
                            server3Message = response.getBody().getObject().getString("value").toString();
                            serverMessage.put("serverC", server3Message);
                            doneLatch.countDown();
                        }

                        public void cancelled() {
                            System.out.println("The request has been cancelled");
                            doneLatch.countDown();
                        }

                    });

        try{
            doneLatch.await(3, TimeUnit.SECONDS);
        }catch(InterruptedException e){

        }

        int countA = getFrequencyByValue(serverMessage, server1Message);
        int countB = getFrequencyByValue(serverMessage, server2Message);
        int countC = getFrequencyByValue(serverMessage, server3Message);

        String finalReply = "";

        if(countA >= 2){ finalReply = server1Message; }
        if(countB >= 2){ finalReply = server2Message; }
        if(countC >= 2){ finalReply = server3Message; }

        if(countA == 1 ){
            finalReply = serverMessage.get("serverB");
            makeRepairCall("http://localhost:3000", key, finalReply);
        }else if(countB == 1){
            finalReply = serverMessage.get("serverC");
            makeRepairCall("http://localhost:3001",key,finalReply);
        }else if(countC == 1 ){
            finalReply = serverMessage.get("serverA");
            makeRepairCall("http://localhost:3002",key,finalReply);
        }
        return finalReply;
    }

    //get frequency of messages for majority vote
    private int getFrequencyByValue(HashMap<String, String> map, String value){
        int count = 0;
        for(Map.Entry<String, String> entry : map.entrySet()){
            if( entry.getValue().equals(value) ){
                count++;
            }
        }
        return count;
    }

    //Make Repair Call
    private void makeRepairCall(String url, long key, String value){
        HttpResponse<JsonNode> response = null;
        System.out.println("Repairing server "+ url +"/cache with {"+key+", "+value+"}");
        try {
            response = Unirest
                    .put(url + "/cache/{key}/{value}")
                    .header("accept", "application/json")
                    .routeParam("key", Long.toString(key))
                    .routeParam("value", value).asJson();
        } catch (UnirestException e) {
            System.err.println(e);
        }
    }

    /**
     * @see edu.sjsu.cmpe.cache.client.CacheServiceInterface#put(long,
     *      java.lang.String)
     */
    @Override
    public void put(long key, String value) {

        doneLatch = new CountDownLatch(3);
        System.out.println("Value : " + value);
        Future<HttpResponse<JsonNode>> server1 = Unirest
                .put("http://localhost:3000/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        isServer1Complete=true;
                        doneLatch.countDown();
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        successCount++;
                        isUp1 = true;
                        isServer1Complete=true;
                        doneLatch.countDown();
                    }

                    public void cancelled() {
                        System.out.println("The request has been cancelled");
                    }

                });

        Future<HttpResponse<JsonNode>> server2 = Unirest
                .put("http://localhost:3001/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        isServer2Complete=true;
                        System.out.println("The request has failed");
                        doneLatch.countDown();
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        successCount++;
                        isUp2 = true;
                        isServer2Complete=true;
                        doneLatch.countDown();
                    }

                    public void cancelled() {
                        System.out.println("The request has been cancelled");
                    }

                });

        Future<HttpResponse<JsonNode>> server3 = Unirest
                .put("http://localhost:3002/cache/{key}/{value}")
                .header("accept", "application/json")
                .routeParam("key", Long.toString(key))
                .routeParam("value", value)
                .asJsonAsync(new Callback<JsonNode>() {

                    public void failed(UnirestException e) {
                        isServer3Complete=true;
                        System.out.println("The request has failed");
                        doneLatch.countDown();
                    }

                    public void completed(HttpResponse<JsonNode> response) {
                        successCount++;
                        isUp3 = true;
                        isServer3Complete=true;
                        doneLatch.countDown();
                    }
                    public void cancelled() {
                        System.out.println("The request has been cancelled");
                    }

                });
        try{
            doneLatch.await(3, TimeUnit.SECONDS);
        }catch(InterruptedException e){

        }
        if(successCount < 2){
            HttpResponse<JsonNode> response;
            try{
                System.out.println("Rolling back due to partial write");
                if(isUp1){
                    System.out.println("Deleting from server 1");
                    response = Unirest.delete("http://localhost:3000/cache/" + key).asJson();
                }
                if(isUp2){
                    System.out.println("Deleting from server 2");
                    response = Unirest.delete("http://localhost:3001/cache/" + key).asJson();
                }
                if(isUp3){
                    System.out.println("Deleting from server 3");
                    response = Unirest.delete("http://localhost:3002/cache/" + key).asJson();
                }
            }catch(UnirestException unirestException){
                System.out.println(unirestException.toString());
            }

        }
        isServer1Complete = isServer2Complete = isServer3Complete = false;
        successCount = 0;
        server1 = server2 = server3 = null;
    }


}
