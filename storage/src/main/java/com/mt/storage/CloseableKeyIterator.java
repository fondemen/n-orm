package com.mt.storage;

import java.util.Iterator;

public interface CloseableKeyIterator extends Iterator<String> {

	public abstract void close();

}