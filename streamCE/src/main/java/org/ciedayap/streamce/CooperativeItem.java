/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce;

import java.time.ZonedDateTime;

/**
 *
 * @author mjdivan
 */
public class CooperativeItem extends BaseItem{
    
    public CooperativeItem(ZonedDateTime generationTimestamp, String attributeIDs[],Object values[]) throws DataStreamItemException
    {               
       super(generationTimestamp,attributeIDs,values);
    }
    
    public String getAttributeID(int i)
    {
        if(attributeIDs==null) return null;
        return (i>=0 && i<attributeIDs.length)? attributeIDs[i]:null;        
    }
    
    public Object getValueAt(int i)
    {
        if(values==null) return null;
        return (i>=0 && i<values.length)? values[i]:null;                
    }
    
    public boolean setValueAt(int i,Object value)
    {
        if(i>=0 && i<values.length) 
        {
            values[i]=value;
            return true;
        }
        
        return false;
    }
      
    public static final CooperativeItem create(ZonedDateTime generationTimestamp, String attributeIDs[],Object values[]) throws DataStreamItemException
    {
        return new CooperativeItem(generationTimestamp,attributeIDs,values);
    }   
}

