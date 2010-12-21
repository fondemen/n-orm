package com.mt.storage.simplestoragefile;

import com.mt.storage.Key;
import com.mt.storage.Persisting;

@Persisting
public class Element {
	@Key public final byte key = '\0';
}
