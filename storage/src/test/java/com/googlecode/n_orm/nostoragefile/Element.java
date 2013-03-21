package com.googlecode.n_orm.nostoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = 8318861181606443432L;
	@Key public byte key = '\0';
}
