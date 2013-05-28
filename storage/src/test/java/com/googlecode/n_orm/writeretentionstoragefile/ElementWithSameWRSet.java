package com.googlecode.n_orm.writeretentionstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting(writeRetentionMs=3)
public class ElementWithSameWRSet {
	private static final long serialVersionUID = 2412246408816651678L;
	@Key public byte key = '\0';
}
