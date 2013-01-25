package com.googlecode.n_orm.referencestoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = -8212096009521630391L;
	@Key public byte key = '\0';
}
