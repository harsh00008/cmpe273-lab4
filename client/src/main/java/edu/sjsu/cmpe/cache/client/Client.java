package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("CRDT Client...");
        CRDTClient client = new CRDTClient();
        System.out.println("Putting(1 => foo)");
        client.put(1, "foo");
        System.out.println("Sleeping for 20 seconds");
        Thread.sleep(20000);

        System.out.println("putting(2 => foo2)");
        client.put(1,"foo2");
        System.out.println("Sleeping for 20 seconds");
        Thread.sleep(20000);

        String value = client.get(1);
        System.out.println("get(1) => " + value);

    }

}
