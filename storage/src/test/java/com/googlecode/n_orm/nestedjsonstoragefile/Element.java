package com.googlecode.n_orm.nestedjsonstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = -5410668506755329190L;
	@Key public byte key = '\0';
}
