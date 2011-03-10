package org.norm.nostoragefile;

import org.norm.Key;
import org.norm.Persisting;

@Persisting
public class Element {
	@Key public byte key = '\0';
}
