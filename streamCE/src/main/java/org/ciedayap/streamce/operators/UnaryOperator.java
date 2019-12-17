/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.ciedayap.streamce.operators;

import java.util.Observer;
import org.ciedayap.streamce.DataStream;
import org.ciedayap.streamce.DataStreamItemException;

/**
 *
 * @author mjdivan
 */
public abstract class UnaryOperator extends Operator implements Observer{
    protected DataStream one;    

    public UnaryOperator(DataStream a) throws OperatorException, DataStreamItemException            
    {
        if(a==null || !a.isReady()) 
            throw new OperatorException("Data Stream is not ready");
        
        one=a;        
    }
    
}
