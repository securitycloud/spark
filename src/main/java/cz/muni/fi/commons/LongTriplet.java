package cz.muni.fi.commons;

import java.io.Serializable;

/**
 * Class representing three Long values.
 * 
 * @author Martin Jelínek (xjeline5)
 */
public class LongTriplet extends Triplet<Long, Long, Long> implements Serializable {
    
    public LongTriplet() {
        super(0L, 0L, 0L);
    }

    public LongTriplet(Long a, Long b, Long c) {
        super(a, b, c);
    }
    
    public LongTriplet add(LongTriplet t) {
        this.a += t.getA();
        this.b += t.getB();
        this.c += t.getC();
        
        return this;
    }
    
    public LongTriplet add(Long a, Long b, Long c) {
        this.a += a;
        this.b += b;
        this.c += c;
        
        return this;
    }

    @Override
    public String toString() {
        return "{" + a.toString() + ", " + b.toString() + ", " + c.toString() + "}";
    }
    
    public String toCsvString() {
        return a.toString() + ";" + b.toString() + ";" + c.toString();
    }
    
}
