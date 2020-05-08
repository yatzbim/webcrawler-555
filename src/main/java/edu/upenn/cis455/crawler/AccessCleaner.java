package edu.upenn.cis455.crawler;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ConcurrentModificationException;

public class AccessCleaner extends Thread {
    
    public static AtomicBoolean flag = new AtomicBoolean(true);
    
    XPathCrawler instance = XPathCrawler.getInstance();
    
    // TODO: IMPORTANT - value stored in last access map should be (new Date().getTime() + delay)
    
    @Override
    public synchronized void run() {
        while (flag.get()) {
            try {
                Thread.sleep(45000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            
            synchronized(XPathCrawler.accessLock) {
                try {
                    Set<String> keys = instance.lastAccessed.keySet();
                    System.out.println("Cleaning old entries in crawltime map: " + keys.size());
                    
                    Set<String> removals = new HashSet<>();
                    
                    for (String hostPort : keys) {
                        if (instance.lastAccessed.get(hostPort) < new Date().getTime()) {
                            removals.add(hostPort);
//                            instance.lastAccessed.remove(hostPort);
                        }
                    }
                    
                    for (String hostPort : removals) {
                        instance.lastAccessed.remove(hostPort);
                    }
                    
                } catch (ConcurrentModificationException e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }
    }
}
