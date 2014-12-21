package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("CRDT Client...");
        CRDTClient client = new CRDTClient();
        System.out.println("putting(1 => foo)");
        client.put(1,"foo");
        Thread.sleep(30000);

        System.out.println("putting(2 => foo2)");
        client.put(2,"foo2");
        Thread.sleep(30000);

        String value = client.get(2);
        System.out.println("get(2) => " + value);

    }

}
