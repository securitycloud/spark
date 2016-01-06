package cz.muni.fi.commons;

import java.io.Serializable;

/**
 * Class representing three values.
 */
public class Triplet<T, U, V> implements Serializable {
    protected T a;
    protected U b;
    protected V c;

    public Triplet(T a, U b, V c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public T getA() {
        return a;
    }

    public U getB() {
        return b;
    }

    public V getC() {
        return c;
    }

    @Override
    public String toString() {
        return "{a=" + a +
                ", b=" + b +
                ", c=" + c +
                '}';
    }
}
