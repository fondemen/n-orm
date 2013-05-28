package com.googlecode.n_orm.writeretentionstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = -4440825567119985854L;
	@Key public byte key = '\0';
}
