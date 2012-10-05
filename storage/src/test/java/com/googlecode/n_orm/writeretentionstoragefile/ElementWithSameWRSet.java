package com.googlecode.n_orm.writeretentionstoragefile;

import com.googlecode.n_orm.Key;
import com.googlecode.n_orm.Persisting;

@Persisting(writeRetentionMs=3)
public class ElementWithSameWRSet {
	@Key public byte key = '\0';
}
