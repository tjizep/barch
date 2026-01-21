package org.barchtest;

import org.barch.KeyValue;
import org.barch.barchJNI;


import java.io.IOException;
import java.util.*;

import static org.barch.barchJNI.clearAll;

public class Main {
    static{
        System.loadLibrary("barchjni");
    }
    static void main() throws InterruptedException, IOException {
        final int count = 1000000;
        var strings = RandomStrings.generateStrings(count,8);
        KeyValue conf = new KeyValue("configuration");
        if (conf.size() == 0) {
            System.out.println("creating configuration");
            conf.set("test.ordered", "1");
            conf.set("test.shards", "1"); // more shards is more concurrency, but only for writes, reads are concurrent- but this is single threaded
        }else {
            System.out.println("configuration already exists");

        }
        KeyValue kv = new KeyValue("test");
        System.out.println("ordered: "+kv.getOrdered());
        System.out.println("shards: "+kv.getShards());
        System.out.println("current size:"+kv.size());
        kv.clear();

        var m = new TreeMap<String,String>();
        //System.in.read();
        long t = System.currentTimeMillis();
        List<Thread> list = new ArrayList<>();
        int threads = 1;
        if (threads == 1) {

            for (int j = 0; j < count; j++) {
                String v = strings[j];
                kv.put(v, "D" + v);
            }
            System.out.println("time for barch: "+(System.currentTimeMillis()-t));
            var mk = kv.min().s();
            var xk = kv.max().s();
            System.out.println("min key: " + mk);
            System.out.println("max key: " + xk);
            System.out.println("count: "+mk+" to "+xk+":" + kv.count(mk,xk));

            var rnd = (int)(Math.random()*count);
            System.out.println("size: " + kv.size() + " check key #("+rnd+") ["+strings[rnd]+"]:" + kv.get(strings[rnd]));
            t = System.currentTimeMillis();
            for (int j = 0; j < count; j++) {
                String v = strings[j];
                m.put(v, "D" + v);
            }
            System.out.println("time for tree: "+(System.currentTimeMillis()-t));
            System.out.println("min key: " + m.firstKey());
            System.out.println("max key: " + m.lastKey());
            System.out.println("size tree: " + m.size() + " check key #("+rnd+") ["+strings[rnd]+"]:" + m.get(strings[rnd]));

        }else {
            for (int i = 0; i < threads; i++) {
                int finalI = i;
                Thread vThread = Thread.ofVirtual()
                        .start(() -> {
                            KeyValue kvt = new KeyValue("test");
                            int start = finalI * count;
                            for (int j = start; j < start + count; j++) {
                                String v = Double.toString(10*(j + Math.random())) ;
                                kvt.put(v, "D" + v);
                            }
                        });
                list.add(vThread);
            }
            for (Thread vThread : list) {
                vThread.join();
            }
        }

        System.out.println("ok");
        kv.clear();
        //barchJNI.saveAll();

    }
}
