package com.googlecode.n_orm.singletonstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = -2227861337288272177L;
	@Key public byte key = '\0';
}
