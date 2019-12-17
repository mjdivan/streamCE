/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce.operators;

import java.time.ZonedDateTime;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;
import org.ciedayap.streamce.CooperativeItem;
import org.ciedayap.streamce.DataStream;
import org.ciedayap.streamce.DataStreamItemException;
import org.ciedayap.streamce.ExclusiveItem;

/**
 *
 * @author mjdivan
 */
public class Projection extends UnaryOperator{
    protected final DataStream result;
    protected String projected[];
    
    protected final java.util.concurrent.ConcurrentHashMap<String, Integer> indexof;    
    protected Integer lookAt[];
    
    public Projection(DataStream a,String attr[]) throws OperatorException, DataStreamItemException {
        super(a);
        if(attr==null || attr.length==0) throw new OperatorException("No Attributes Specified");
        
        String att_o[]=a.getAttributeIDs();
        indexof=new ConcurrentHashMap(att_o.length+5);
        for(int i=0;i<att_o.length;i++)
        {
            indexof.put(att_o[i], i);
        }
        
        projected=attr;
        lookAt=new Integer[projected.length];
        for(int i=0;i<projected.length;i++)
        {
            Integer idx= indexof.get(projected[i]);
            if(idx==null) throw new OperatorException("Attribute '"+projected[i]+"' no present in the original data stream");
            else lookAt[i]=idx;            
        }
        
        result=DataStream.create("proj_"+a.getName(), projected, true);
    }
    
    public void addResultObserver(Observer ob)
    {
        result.addObserver(ob);
    }

    @Override
    public void update(Observable o, Object arg) {
        long currentTime=System.nanoTime();
        
        CooperativeItem ci=null;
        ExclusiveItem ei=null;
        if(arg instanceof CooperativeItem)
        {
            ci=(CooperativeItem)arg;
        }
        else
            if(arg instanceof ExclusiveItem)
            {
                ei=(ExclusiveItem)arg;
            }
            else {
                return;
            }
            
        CooperativeItem iresult;
        try {
             iresult=(CooperativeItem)result.emptyItem();
        } catch (DataStreamItemException ex) {
            return;
        }
        
        DataStream ds=(DataStream)o;
        
        if(ds.getName().equalsIgnoreCase(one.getName()))
        {//Updating the left DS
            if(ci!=null)
            {
                iresult.setTimestamp(ci.getTimestamp());
                
                Object uitem[]=ci.getValues();
                for(int i=0;i<lookAt.length;i++)
                    iresult.setValueAt(i, uitem[lookAt[i]]);                
            }
            else
            {                
                iresult.setTimestamp(ei.getTimestamp());
                iresult.setValueAt(0, ei.getValueAt());                
            }
                                        
                
            result.addCooperativeItem(iresult);
            
            long lastTime=System.nanoTime();
            
            System.out.println(ZonedDateTime.now()+"\t"+(lastTime-currentTime));
        }        
    }
 
  public synchronized static Projection create(DataStream a,String att[]) throws OperatorException, DataStreamItemException
    {
        Projection p=new Projection(a,att);
        a.addObserver(p);
        
        return p;
    }
       
}
