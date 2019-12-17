/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce.operators;

import java.time.ZonedDateTime;
import java.util.Observable;
import org.ciedayap.streamce.CooperativeItem;
import org.ciedayap.streamce.DataStream;
import org.ciedayap.streamce.DataStreamItemException;
import org.ciedayap.streamce.ExclusiveItem;

/**
 *
 * @author mjdivan
 */
public class Union extends BinaryOperator{
    
    public Union(DataStream one, DataStream two) throws OperatorException, DataStreamItemException
    {
        super(one,two);
        this.name="Union";
        this.symbol="U";
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
            int i=0;
            if(ci!=null)
            {
                iresult.setTimestamp(ci.getTimestamp());
                
                Object uitem[]=ci.getValues();
                for(;i<uitem.length;i++)
                    iresult.setValueAt(i, uitem[i]);                
            }
            else
            {                
                iresult.setTimestamp(ei.getTimestamp());
                iresult.setValueAt(0, ei.getValueAt());                
                i++;
            }
                                        
            Object two_item=two.lastValue();
            if(two_item!=null)
            {
                if(two_item instanceof CooperativeItem)
                {
                    CooperativeItem right=(CooperativeItem)two_item;
                    Object[] rvalues=right.getValues();
                    for(int j=0;j<rvalues.length;j++)
                    {
                        iresult.setValueAt(i, rvalues[j]);
                        i++;
                    }
                }
                else
                {
                    ExclusiveItem right=(ExclusiveItem)two_item;
                    iresult.setValueAt(i, right.getValueAt());
                    i++;        
                }
                
                result.addCooperativeItem(iresult);
            }
            else 
            {
                result.addCooperativeItem(iresult);
            }
            
            long lastTime=System.nanoTime();
            System.out.println(ZonedDateTime.now()+"\t"+(lastTime-currentTime));

            return;//Udpdated result
        }

        if(ds.getName().equalsIgnoreCase(two.getName()))
        {//Updating the right DS                 
            int i=one.getAttributeIDs().length;
            if(ci!=null)
            {
                iresult.setTimestamp(ci.getTimestamp());
                
                Object uitem[]=ci.getValues();
                for(int j=0;j<uitem.length;j++)
                {
                    iresult.setValueAt(i, uitem[j]);                
                    i++;
                }
            }
            else
            {                
                iresult.setTimestamp(ei.getTimestamp());
                iresult.setValueAt(i, ei.getValueAt());                
                i++;
            }
                                        
            i=0;//Update the left side
            Object one_item=one.lastValue();
            if(one_item!=null)
            {
                if(one_item instanceof CooperativeItem)
                {
                    CooperativeItem left=(CooperativeItem)one_item;
                    Object[] rvalues=left.getValues();
                    for(int j=0;j<rvalues.length;j++)
                    {
                        iresult.setValueAt(i, rvalues[j]);
                        i++;
                    }
                }
                else
                {
                    ExclusiveItem left=(ExclusiveItem)one_item;
                    iresult.setValueAt(i,left.getValueAt());
                    i++;        
                }
                
                result.addCooperativeItem(iresult);
            }
            else 
            {                
                result.addCooperativeItem(iresult);
            }        
            
            long lastTime=System.nanoTime();                        
            System.out.println(ZonedDateTime.now()+"\t"+(lastTime-currentTime));
        } 
        
    }
    
    public synchronized static Union create(DataStream a,DataStream b) throws OperatorException, DataStreamItemException
    {
        Union u=new Union(a,b);
        a.addObserver(u);
        b.addObserver(u);
        
        return u;
    }
    
    
}
