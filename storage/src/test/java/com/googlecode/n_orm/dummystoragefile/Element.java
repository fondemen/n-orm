package com.googlecode.n_orm.dummystoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	@Key public final byte key = '\0';
}
