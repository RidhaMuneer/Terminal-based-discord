package com.example;

import java.time.Instant;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.Timestamp;

public class ClientUtil {

    private static int counter = 0;

    public static void run() throws Exception{
                CountDownLatch lc = new CountDownLatch(1);
       

         Watcher wc = new Watcher() {
            public void process(WatchedEvent event) {

            }
        };



        ZooKeeper zoo = new ZooKeeper("127.0.0.1", 2181, wc);


        // Watcher watcher = new Watcher() {
        //     public void process(WatchedEvent event) {
        //             if(event.getType() == Watcher.Event.EventType.NodeCreated) {
        //                 try{
        //                     byte[] data = zoo.getData("/chatroom", wc, null); 
        //                     Message receivedMessage = Message.parseFrom(data); 
        //                     System.out.printf("[%s] %s: %s", receivedMessage.getTime().toString(), receivedMessage.getUsername(), receivedMessage.getContent());
        //                 } catch(Exception e) {
        //                     e.printStackTrace();
        //                 }
                       
        //         }
        //     }
        // };


        Scanner scanner = new Scanner(System.in); 
        System.out.print("Enter your name: ");
        String name = scanner.nextLine();
 

        
        String path = "/" + name; 
        byte[] byteData = name.getBytes(); 
        Stat s = zoo.exists(path, true);
        if(s==null) {
            zoo.create(path, byteData, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
        else {
            zoo.setData(path, byteData, -1);
        }

        // String messagesPath = path + "/messages"; 
        // zoo.create(messagesPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); 
        Stat chatroomStat = zoo.exists("/chatroom", false);
        if(chatroomStat == null) zoo.create("/chatroom", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            zoo.addWatch("/chatroom", new Watcher() {
            public void process(WatchedEvent event) {
                    if(event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                        try{
                            List<String> children = zoo.getChildren("/chatroom", false); 
                            String childPath = "/chatroom/"+children.get(children.size() - 1); 
                            System.out.println("+++++++++++++++++++++++++++++++++++path:"+event.getPath());
                            byte[] data = zoo.getData(childPath, wc, null); 
                            Message receivedMessage = Message.parseFrom(data); 
                            // System.out.printf("[%s] %s: %s", receivedMessage.getTime().toString(), receivedMessage.getUsername(), receivedMessage.getContent());
                            System.out.println("+++++++++++++++++++++++++++++++++++====+++++++++"+receivedMessage.getContent());
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                       
                }
            }
        }, AddWatchMode.PERSISTENT);
            // int counter = 0;
                   Stat stat = zoo.exists("/chatroom/counter", false);
        if (stat == null) {
            zoo.create("/chatroom/counter", "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            byte[] data = zoo.getData("/chatroom/counter", false, null);
            counter = Integer.parseInt(new String(data));
        }
            while(true) {
                System.out.print("Enter a message: "); 
                String msgContent = scanner.nextLine(); 


                Instant instant = Instant.now(); 
                Timestamp timestamp = Timestamp.newBuilder()
                                            .setSeconds(instant.getEpochSecond())
                                            .setNanos(instant.getNano())
                                            .build();


                Message msg = Message.newBuilder()
                                    .setUsername(name)
                                    .setTime(timestamp)
                                    .setContent(msgContent)
                                    .build();

                String chatroomMsgPath = "/chatroom" + "/"+name+"message" +counter;
                // String userMsgPath = messagesPath + "/message" + counter; 
                byte[] data = msg.toByteArray(); 
                
                zoo.create(chatroomMsgPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                // zoo.create(userMsgPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                // zoo.setData(messagesPath, msg.toByteArray(), 0);
                byte[] newData = String.valueOf(counter).getBytes();
        zoo.setData("/chatroom/counter", newData, -1);
                counter++;
                // lc.await();
        }

    
    // lc.await();
    }

      private static void initializeCounter() throws KeeperException, InterruptedException {
 
    }

    private static void updateCounter() throws KeeperException, InterruptedException {
        
    }
}


