package io.digdag.core;

import java.util.List;

public interface Pageable <T>
{
    public List<T> next();
    //public void skip(int count);
}
