/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce;

import java.io.Serializable;
import java.time.ZonedDateTime;

/**
 *
 * @author mjdivan
 */
public abstract class BaseItem implements Serializable{
    protected java.time.ZonedDateTime timestamp;
    protected String attributeIDs[];
    protected Object values[];

    public BaseItem()
    {
        timestamp=ZonedDateTime.now();
    }
    
    public BaseItem(ZonedDateTime generationTimestamp, String atts[],Object vals[]) throws DataStreamItemException
    {
        if(generationTimestamp==null) throw new DataStreamItemException("Timestamp cannot be null");        
        if(atts.length!=vals.length) throw new DataStreamItemException("Attribute and Values lengths are different");        
        timestamp=generationTimestamp;
        attributeIDs=atts;
        values=vals;
    }
    
    /**
     * @return the timestamp
     */
    public java.time.ZonedDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(java.time.ZonedDateTime timestamp) {
        this.timestamp = timestamp;
    }
    
    public boolean isHeaderEquivalent(String otherAttributeIDs[])
    {
        if(otherAttributeIDs==null || attributeIDs==null) return false;
        if(attributeIDs.length!= otherAttributeIDs.length) return false;
        for(int i=0;i<otherAttributeIDs.length;i++)
            if(otherAttributeIDs[i]==null || attributeIDs[i]==null ||
                    !otherAttributeIDs[i].equalsIgnoreCase(attributeIDs[i])) return false;
        
        return true;                
    }    
    
    public boolean isConsistent()
    {
        return (attributeIDs!=null && values!=null && attributeIDs.length==values.length);
    }
    
    public Object[] getValues()
    {
        return values;
    }
    
    @Override
    public String toString()
    {
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<values.length;i++)
        {
           sb.append("{").append(attributeIDs[i]).append(": ").append(values[i]).append("} ");
        }
        
        return sb.toString();
    }
}
