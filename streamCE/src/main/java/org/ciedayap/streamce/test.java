/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.ciedayap.streamce.operators.OperatorException;
import org.ciedayap.streamce.operators.Projection;
import org.ciedayap.streamce.operators.Union;

/**
 *
 * @author mjdivan
 */
public class test implements Runnable{
    private final DataStream stream;
    private boolean stop=false;
    
    public test(DataStream s)
    {
        stream=s;        
    }
    
    public void setStop(boolean s)
    {
        stop=s;
    }
    

    public static void main(String args[]) throws DataStreamItemException, OperatorException
    {
        //unionTest();
        projectionTest();
    }

    public static void unionTest() throws DataStreamItemException, OperatorException
    {
        String ds_a[]={"a","b","c"};
        DataStream streamA=DataStream.create("streamA", ds_a, true);
        
        String ds_b[]={"d","e","f","g","h","i"};
        DataStream streamB=DataStream.create("streamB", ds_b, true);
        
        Union opu=Union.create(streamA, streamB);
        
        ExecutorService pool = Executors.newFixedThreadPool(2);
        test t1,t2;
        t1=new test(streamA);
        t2=new test(streamB);
        pool.execute(t1);
        pool.execute(t2);
        
        long current,start=System.currentTimeMillis();
        do{
            current=System.currentTimeMillis();
        }while((current-start)<=(1000*60*10));
        
        t1.setStop(true);
        t2.setStop(true);
        pool.shutdown();        
    }
    
    public static void projectionTest() throws OperatorException, DataStreamItemException
    {
        String ds_a[]={"a","b","c","d","e","f","g","h","i"};
        String projected[]={"c","e","i","h"};
        DataStream streamA=DataStream.create("streamA", ds_a, true);
        
        Projection proj=Projection.create(streamA,projected);
        
        ExecutorService pool = Executors.newFixedThreadPool(1);
        test t1;
        t1=new test(streamA);

        pool.execute(t1);       
        
        long current,start=System.currentTimeMillis();
        do{
            current=System.currentTimeMillis();
        }while((current-start)<=(1000*60*10));
        
        t1.setStop(true);
        pool.shutdown();        
    }
    
    @Override
    public void run() {
        System.out.println("Runing "+Thread.currentThread().getName());
        while(!stop)
        {
            CooperativeItem ci=null;
            try {
                 ci=(CooperativeItem) stream.nrandomItem();
            } catch (DataStreamItemException ex) {
                System.out.println(ex.getMessage());
            }
            
            stream.addCooperativeItem(ci);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                Logger.getLogger(test.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        System.out.println("Finishing "+Thread.currentThread().getName());
    }
}
