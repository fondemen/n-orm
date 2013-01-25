package com.googlecode.n_orm.simplestoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element2 {
	private static final long serialVersionUID = 758189870997119025L;
	@Key public byte key = '\0';
}
