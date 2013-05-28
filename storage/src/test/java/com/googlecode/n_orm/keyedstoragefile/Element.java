package com.googlecode.n_orm.keyedstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting
public class Element {
	private static final long serialVersionUID = -1021944097532358094L;
	@Key public byte key = '\0';
}
