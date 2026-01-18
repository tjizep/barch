package org.barchtest;

import org.barch.KeyValue;
import org.barch.barchJNI;


import static org.barch.barchJNI.clearAll;

public class Main {
    static{
        String path = "/home/teejip/Desktop/barch/cmake-build-release/";
        System.out.println(path);
        //System.setProperty("java.library.path",path);
        System.loadLibrary("barchjni");
    }
    static void main() {

        KeyValue conf = new KeyValue("configuration");
        conf.set("test.ordered", "1");
        conf.set("test.shards", "1"); // more shards is more concurrency, but only for writes, reads are concurrent- but this is single threaded
        KeyValue kv = new KeyValue("test");
        System.out.println("ordered:"+kv.getOrdered());
        System.out.println("current size:"+kv.size());
        kv.clear();
        long t = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            String v = Integer.toString(i);
            kv.put(v, "D"+v);
        }
        System.out.println(System.currentTimeMillis()-t);
        System.out.println("min key:" + kv.min().s());
        System.out.println("max key:" + kv.max().s());
        System.out.println("size:" + kv.size() + " 100:" + kv.get("100"));
        System.out.println("ok");

        clearAll();
        barchJNI.saveAll();

    }
}
