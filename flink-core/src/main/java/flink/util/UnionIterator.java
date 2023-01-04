package flink.util;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @Description
 * @Author weiyu
 * @Version V1.0.0
 * @Since 1.0
 * @Date 12/22/2022
 */
public class UnionIterator <T> implements Iterator<T>, Iterable<T>{

    private Iterator<T> currentIterator;

    private ArrayList<Iterator<T>> furtherIterators = new ArrayList<>();

    private int nextIterator;

    private boolean iteratorAvailable = true;



    @Override
    public Iterator<T> iterator() {
        return null;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        return null;
    }


    public void add(Iterator<T> iterator) {
        if (currentIterator == null) {
            currentIterator = iterator;
        } else {
            furtherIterators.add(iterator);
        }
    }
}
