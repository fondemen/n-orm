package com.googlecode.n_orm.referencestoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	@Key public byte key = '\0';
}
