package org.barchtest;

import org.barch.KeyValue;
import org.barch.barchJNI;


import java.util.ArrayList;
import java.util.List;

import static org.barch.barchJNI.clearAll;

public class Main {
    static{
        String path = "/home/teejip/Desktop/barch/cmake-build-release/";
        System.out.println(path);
        //System.setProperty("java.library.path",path);
        System.loadLibrary("barchjni");
    }
    static void main() throws InterruptedException {

        KeyValue conf = new KeyValue("configuration");
        if (conf.size() == 0) {
            System.out.println("creating configuration");
            conf.set("test.ordered", "1");
            conf.set("test.shards", "8"); // more shards is more concurrency, but only for writes, reads are concurrent- but this is single threaded
        }else {
            System.out.println("configuration already exists");

        }
        KeyValue kv = new KeyValue("test");
        System.out.println("ordered:"+kv.getOrdered());
        System.out.println("current size:"+kv.size());
        kv.clear();
        long t = System.currentTimeMillis();
        List<Thread> list = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            int finalI = i;
            Thread vThread = Thread.ofVirtual()
                .name("my-worker-", finalI) // Optional: prefix and starting index
                .start(() -> {
                    KeyValue kvt = new KeyValue("test");
                    int start = finalI *1000000;
                    for (int j = start; j < start + 1000000; j++) {
                        String v = Integer.toString(j);
                        kvt.put(v, "D"+v);
                    }
                });
            list.add(vThread);
        }
        for (Thread vThread : list){
            vThread.join();
        }


        System.out.println("time:"+(System.currentTimeMillis()-t));
        System.out.println("min key:" + kv.min().s());
        System.out.println("max key:" + kv.max().s());
        System.out.println("size:" + kv.size() + " 100:" + kv.get("100"));
        System.out.println("count:" + kv.count("100","10000000"));
        System.out.println("ok");

        barchJNI.saveAll();

    }
}
