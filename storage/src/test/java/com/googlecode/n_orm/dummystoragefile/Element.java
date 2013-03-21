package com.googlecode.n_orm.dummystoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = -4700717707196622658L;
	@Key public final byte key = '\0';
}
