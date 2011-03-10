package org.norm.dummystoragefile;

import org.norm.Key;
import org.norm.Persisting;

@Persisting
public class Element {
	@Key public final byte key = '\0';
}
