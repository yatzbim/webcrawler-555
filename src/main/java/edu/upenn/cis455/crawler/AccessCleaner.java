package edu.upenn.cis455.crawler;

import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
            
            Set<String> keys = instance.lastAccessed.keySet();
            synchronized(XPathCrawler.accessLock) {
                System.out.println("Cleaning old entries in crawltime map: " + keys.size());
                for (String hostPort: keys) {
                    if (instance.lastAccessed.get(hostPort) < new Date().getTime()) {
                        instance.lastAccessed.remove(hostPort);
                    }
                }
            }
        }
    }
}
