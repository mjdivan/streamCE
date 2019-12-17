/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce.operators;

import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;
import org.ciedayap.streamce.DataStream;
import org.ciedayap.streamce.DataStreamItemException;

/**
 *
 * @author mjdivan
 */
public abstract class BinaryOperator extends Operator implements Observer{
    protected final DataStream one;
    protected final DataStream two;
    
    protected final DataStream result;
    
    protected final String attributeIDs[];
    protected final java.util.concurrent.ConcurrentHashMap<String, Integer> indexof;
    
    
    public BinaryOperator(DataStream a,DataStream b) throws OperatorException, DataStreamItemException
    {
        if(a==null || b==null || !a.isReady() || !b.isReady()) 
            throw new OperatorException("Data Streams are not ready");
        
        one=a;
        two=b;    
        
        String att_one[]=one.getAttributeIDs();
        
        String att_two[]=two.getAttributeIDs();
        attributeIDs=new String[att_one.length+att_two.length];
        indexof=new ConcurrentHashMap(attributeIDs.length+5);
        
        fusioningAttributeIDs(att_one,att_two);
        
        result=DataStream.create(a.getName()+"_U_"+b.getName(),attributeIDs, true);
    }
    
    private void fusioningAttributeIDs(String att_one[], String att_two[]) throws OperatorException
    {
        int i=0;
        for(;i<att_one.length;i++)
        {
            attributeIDs[i]=(att_one[i]);
            indexof.put(att_one[i], i);
        }

        for(int j=0;j<att_two.length;j++)
        {
            if(indexof.containsKey(att_two[j])) 
               throw new OperatorException("Attribute repeated. Attribute '"+att_two[j]+"' is present in the first data stream. Rename the last one.");
            
            attributeIDs[i]=(att_two[j]);
            indexof.put(att_two[j], i);
            i++;
        }                
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
    
    public void addResultObserver(Observer ob)
    {
        result.addObserver(ob);
    }
        
}
