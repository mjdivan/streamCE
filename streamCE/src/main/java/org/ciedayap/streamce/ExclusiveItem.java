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
public class ExclusiveItem extends BaseItem{
  public ExclusiveItem(String attributeID,Object value) throws DataStreamItemException
    {               
       if(attributeID==null) throw new DataStreamItemException("Attribute without ID");
       String myAttr[]=new String[1];
       myAttr[0]=attributeID;
       Object vals[]=new Object[1];
       vals[0]=value;
       
       this.timestamp=ZonedDateTime.now();
       this.values=vals;
       this.attributeIDs=myAttr;
    }
    
    public String getAttributeID()
    {
        if(attributeIDs==null) return null;
        return attributeIDs[0];        
    }
    
    public Object getValueAt()
    {
        if(values==null) return null;
        return values[0];                
    }
    
    public boolean setValueAt(Object value)
    {
        if(values==null) return false;
        values[0]=value;
    
        return true;
    }
    
    public static final ExclusiveItem create(String attributeID,Object value) throws DataStreamItemException
    {
        return new ExclusiveItem(attributeID,value);
    }  
}
