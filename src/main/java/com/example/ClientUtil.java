package com.example;

// Ridha Muneer Kamil 20347
// Mohammed Salih 20533

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ClientUtil {

    static ArrayList<String> messages = new ArrayList<String>();

    public static void run() throws Exception {
        String name = getUsername();
        connectToServer(name);
    }

    public static void connectToServer(String name) throws Exception {
        CountDownLatch lc = new CountDownLatch(1);
        Watcher wc = new Watcher() {
            public void process(WatchedEvent event) {
                
            }
        };
        ZooKeeper zoo = new ZooKeeper("127.0.0.1", 2181, wc);
        createUserNode(zoo, wc, name);
        createChatroom(zoo, wc, name);
        while (true) {
            sendMessage(zoo, name, lc);
            if(lc.getCount() == 0){
                break;
            }
            lc.await();
        }
    }

    public static void createUserNode(ZooKeeper zoo, Watcher wc, String name) throws Exception {
        String path = "/" + name; 
        byte[] byteData = name.getBytes(); 
        Stat s = zoo.exists(path, true);
        if(s==null) {
            zoo.create(path, byteData, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        else {
            zoo.setData(path, byteData, -1);
        }
        checkForNewUsers(zoo, name);
        getPreviousMessages(zoo, wc);
    }

    public static void getPreviousMessages(ZooKeeper zoo, Watcher wc) throws KeeperException, InterruptedException, ClassNotFoundException, IOException{
        byte[] data = zoo.getData("/chatroom", wc, null); 
        ArrayList<String> messageDB = deserialize(data);
        for (String item : messageDB) {
            System.out.println(item);
        }
    }

    public static void createChatroom(ZooKeeper zoo, Watcher wc, String name) throws Exception {
        Stat chatroomStat = zoo.exists("/chatroom", false);
        if(chatroomStat == null) zoo.create("/chatroom", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zoo.addWatch("/chatroom", new Watcher() {
            public void process(WatchedEvent event) {
                if(event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                    try{
                        byte[] data = zoo.getData("/chatroom", wc, null); 
                        ArrayList<String> messageDB = deserialize(data);
                        for (String item : messageDB) {
                            System.out.println(item);
                        }
                    } catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }, AddWatchMode.PERSISTENT);
    }

    public static void checkForNewUsers(ZooKeeper zoo, String name) throws Exception {
        zoo.addWatch("/", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() != Watcher.Event.EventType.NodeCreated) {
                    try {
                        String connectedMessage = "- User " + name + " has been connected -";
                        messages.add(connectedMessage);
                        Stat stat = new Stat();
                        byte[] currentData = zoo.getData("/chatroom", false, stat);
                        ArrayList<String> chatroomMessages = deserialize(currentData);
                        chatroomMessages.add(connectedMessage);
                        byte[] listOfMessages = serialize(chatroomMessages);
                        zoo.setData("/chatroom", listOfMessages, stat.getVersion());
                    } catch (KeeperException.NoNodeException e) {
                        System.out.println("Node does not exist. It might have been deleted by another process.");
                    } catch (KeeperException.BadVersionException e) {
                        System.out.println("Another client modified the node concurrently.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }, AddWatchMode.PERSISTENT);
    }

    public static void sendDisconnectedMessage(ZooKeeper zoo, String name, CountDownLatch lc) throws KeeperException, InterruptedException, IOException, ClassNotFoundException {
        String disconnectedMessage = "- User " + name + " has been disconnected -";
        messages.add(disconnectedMessage);
        Stat chatroomStat = new Stat();
        byte[] currentData = zoo.getData("/chatroom", false, chatroomStat);
        ArrayList<String> chatroomMessages = deserialize(currentData);
        if (!chatroomMessages.isEmpty()) {
            String lastMessage = chatroomMessages.get(chatroomMessages.size() - 1);
            String[] parts = lastMessage.split(":");
            if (parts.length >= 2) {
                String wordBeforeColon = parts[0].trim(); 
                if (wordBeforeColon.equals(name)) {
                    chatroomMessages.remove(chatroomMessages.size() - 1);
                } 
            }
        } else {
            System.out.println("Chatroom is empty");
        }
        chatroomMessages.add(disconnectedMessage);
        byte[] listOfMessages = serialize(chatroomMessages);
        zoo.setData("/chatroom", listOfMessages, chatroomStat.getVersion());
        zoo.close();
        lc.countDown();
    }

    public static String getUsername(){
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter your username: ");
        String name = scanner.nextLine();
        return name;
    }

    public static String askForMessage(){
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter a message: (-1 for EXIT)");
        String message = scanner.nextLine();
        return message;
    }

    public static void sendMessage(ZooKeeper zoo, String name, CountDownLatch lc) throws IOException, KeeperException, InterruptedException, ClassNotFoundException {
        String message = askForMessage();
        if(message.equals("-1")){
            sendDisconnectedMessage(zoo, name, lc);
        }
        Stat stat = new Stat();
        byte[] currentData = zoo.getData("/chatroom", false, stat);

        ArrayList<String> chatroomMessages;

        if (currentData == null || currentData.length == 0) {
            chatroomMessages = new ArrayList<>();
        } else {
            chatroomMessages = deserialize(currentData);
        }
    
        Instant currentTimestamp = Instant.now();
        DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
        String formattedTimestamp = formatter.format(currentTimestamp);
        chatroomMessages.add(name + ": [" + formattedTimestamp + "] " + message);

        byte[] listOfMessages = serialize(chatroomMessages);

        zoo.setData("/chatroom", listOfMessages, stat.getVersion());
    }

    private static byte[] serialize(Object obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(obj);
            return bos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<String> deserialize(byte[] data) throws IOException, ClassNotFoundException {
        if (data == null || data.length == 0) {
            return new ArrayList<>();
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInput in = new ObjectInputStream(bis)) {
            return (ArrayList<String>) in.readObject();
        } catch (Exception e) {
            System.err.println("Error during deserialization:");
            e.printStackTrace();
            throw e; 
        }
    }    
}