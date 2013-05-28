package com.googlecode.n_orm.simplestoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = 662817639038988904L;
	@Key public byte key = '\0';
}
