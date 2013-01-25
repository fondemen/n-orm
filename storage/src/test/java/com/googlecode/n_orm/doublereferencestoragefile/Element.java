package com.googlecode.n_orm.doublereferencestoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = 2061606785758976568L;
	@Key public byte key = '\0';
}
