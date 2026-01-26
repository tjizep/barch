package org.barchtest;

import org.barch.KeyValue;
import org.barch.barch;
import java.io.IOException;
import java.util.*;

public class Main {
    static{
        System.loadLibrary("barchjni");
    }
    static void printStats(KeyValue kv){
        var stats = barch.stats();
        System.out.println("heap bytes allocated:"+stats.getHeap_bytes_allocated());
        System.out.println("vmm bytes allocated:"+stats.getVmm_bytes_allocated());
        System.out.println("logical bytes bytes in free lists:"+stats.getBytes_in_free_lists());
        System.out.println("logical bytes allocated:"+stats.getLogical_allocated());
        System.out.println("vmm pages defragged:"+stats.getVmm_pages_defragged());
        System.out.println("vmm pages popped:"+stats.getVmm_pages_popped());
        System.out.println("keys evicted:"+stats.getKeys_evicted());
        System.out.println("test size:"+kv.size());

    }
    static void main() throws InterruptedException, IOException {
        final int count = 800000;
        final boolean doTree = false;
        final int threads = 1;

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

        if (threads == 1) {

            for (int j = 0; j < count; j++) {
                String v = strings[j];
                kv.put(v, "D" + v + v);
            }
            System.out.println("time for barch: "+(System.currentTimeMillis()-t));
            var mk = kv.min().s();
            var xk = kv.max().s();
            System.out.println("min key: " + mk);
            System.out.println("max key: " + xk);
            System.out.println("count: "+mk+" to "+xk+":" + kv.count(mk,xk));

            var rnd = (int)(Math.random()*count);
            System.out.println("size: " + kv.size() + " check key #("+rnd+") ["+strings[rnd]+"]:" + kv.get(strings[rnd]));

            barch.setConfiguration("max_memory_bytes","2000000");
            kv.setLru("ON");
            for (int i = 0; i < 100; i++) {
                printStats(kv);
                var stats = barch.stats();
                Thread.sleep(1000);
                if (stats.getLogical_allocated() < 1000000){
                    System.out.println("reloading");
                    kv.reload();
                    printStats(kv);
                    break;
                }
            }

            if(doTree) {
                t = System.currentTimeMillis();
                for (int j = 0; j < count; j++) {
                    String v = strings[j];
                    m.put(v, "D" + v + v);
                }
                System.out.println("time for tree: " + (System.currentTimeMillis() - t));
                System.out.println("min key: " + m.firstKey());
                System.out.println("max key: " + m.lastKey());
                System.out.println("size tree: " + m.size() + " check key #(" + rnd + ") [" + strings[rnd] + "]:" + m.get(strings[rnd]));

            }
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
