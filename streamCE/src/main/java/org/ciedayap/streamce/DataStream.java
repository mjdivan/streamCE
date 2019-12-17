/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce;

import java.time.ZonedDateTime;
import java.util.Observable;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author mjdivan
 */
public class DataStream extends Observable{
    protected final String name;
    protected final String attributeIDs[];
    protected final java.util.concurrent.ConcurrentHashMap<String, Integer> indexof;
            
    protected final boolean cooperative;
    protected final LinkedBlockingQueue stream;
    
    protected Object lastValue;
    
    public DataStream(String name,String[] atts,boolean isCoop) throws DataStreamItemException
    {
        if(name==null) throw new DataStreamItemException("No name has been provided");
        if(atts==null) throw new DataStreamItemException("Attributes not defined");
        if(atts.length>1 && isCoop==false) throw new DataStreamItemException("cooperative=false but there is more than one attribute");
        indexof=new java.util.concurrent.ConcurrentHashMap(atts.length+5);
        this.name=name;
        
        for(int i=0;i<atts.length;i++) 
        {
            if(atts[i]==null)
            {
                indexof.clear();
                throw new DataStreamItemException("Attribute "+i+" is null");
            }
            
            if(indexof.contains(atts[i]))
            {
                throw new DataStreamItemException("Attribute "+atts[i]+" is repeated in positions "+i+" and "
                +(indexof.get(atts[i])));
            }
            else
            {
                indexof.put(atts[i], i);
            }
        }
        
        stream=new LinkedBlockingQueue();
        cooperative=isCoop;
        attributeIDs=atts;                        
    }
    
    public synchronized Integer indexOf(String attID)
    {
        return (indexof.containsKey(attID))?indexof.get(attID):null;
    }
    
    public synchronized String indexOf(int pos)
    {
        if(attributeIDs==null) return null;
        if(pos<0 || pos>attributeIDs.length) return null;
        
        return attributeIDs[pos];
    }
    
    private synchronized Boolean add(Object ptr)
    {
        if(ptr==null) return null;
        
        if(ptr instanceof CooperativeItem)
        {
            CooperativeItem last=(CooperativeItem)ptr;
            if(!last.isHeaderEquivalent(attributeIDs)) return false;//header has been changed
            if(!last.isConsistent()) return false;//no consistent
            
            if(!stream.offer(ptr))
            {
                stream.poll();//remove header for                
                
                if(!stream.offer(ptr)) return false;
            }
            
            lastValue=ptr;
            this.setChanged();//mark as a changed value
            this.notifyObservers(ptr);//trigger notification            
            
            return true;//Success in the first intent
        }
        else
        {
            if(ptr instanceof ExclusiveItem)
            {
                ExclusiveItem last=(ExclusiveItem)ptr;
                if(!last.isHeaderEquivalent(attributeIDs)) return false;//header has been changed
                if(!last.isConsistent()) return false;//no consistent

                if(!stream.offer(ptr))
                {
                    stream.poll();//remove header for                

                    if(!stream.offer(ptr)) return false;
                }

                lastValue=ptr;
                this.setChanged();//mark as a changed value
                this.notifyObservers(ptr);//trigger notification            

                return true;//Success in the first intent
            }
            else return null;//
        }
    }
    
    public Object lastValue()
    {
        return lastValue;
    }
    
    public void clear()
    {
        this.stream.clear();
        this.setChanged();//mark as a changed value
        this.notifyObservers();//trigger notification                            
    }
    
    public synchronized Boolean addCooperativeItem(CooperativeItem item)
    {
        return add(item);
    }
    
    public synchronized Boolean addExclusiveItem(ExclusiveItem item)
    {
        return add(item);
    }            
    
    public Object[] recentHistory()
    {
        return (stream!=null)?stream.toArray():null;
    }
    
    public boolean isReady()
    {
        return !(name==null || attributeIDs==null || indexof==null || stream==null); 
    }
    
    public String[] getAttributeIDs()
    {
        return this.attributeIDs.clone();
    }
    
    public static DataStream create(String name,String[] atts,boolean isCoop) throws DataStreamItemException
    {
       return new DataStream(name,atts,isCoop);
    }
    
    public String getName()
    {
        return name;
    }
    
    public Object emptyItem() throws DataStreamItemException
    {
       if(cooperative)
       {
           return CooperativeItem.create(ZonedDateTime.now(), attributeIDs, new Object[attributeIDs.length]);
       }
       else
       {
           return ExclusiveItem.create(attributeIDs[0], new Object());
       }
    }
    
    public Object nrandomItem() throws DataStreamItemException
    {
       Random r=new Random(System.nanoTime());
       
       if(cooperative)
       {

           CooperativeItem ci= CooperativeItem.create(ZonedDateTime.now(), attributeIDs, new Object[attributeIDs.length]);
           for(int i=0;i<attributeIDs.length;i++)
           {
               ci.setValueAt(i, r.nextLong());
           }
           
           return ci;
       }
       else
       {
           ExclusiveItem ei=ExclusiveItem.create(attributeIDs[0], new Object());
           
           ei.setValueAt(r.nextLong());
           
           return ei;           
       }
    }
    
}
