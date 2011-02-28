package com.mt.storage.singletonstoragefile;

import com.mt.storage.Key;
import com.mt.storage.Persisting;

@Persisting
public class Element {
	@Key public byte key = '\0';
}
