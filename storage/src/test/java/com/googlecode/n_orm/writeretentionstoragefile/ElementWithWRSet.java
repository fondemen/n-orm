package com.googlecode.n_orm.writeretentionstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting(writeRetentionMs=4)
public class ElementWithWRSet {
	private static final long serialVersionUID = -8096246298744742660L;
	@Key public byte key = '\0';
}
