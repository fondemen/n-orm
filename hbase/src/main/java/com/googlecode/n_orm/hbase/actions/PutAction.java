package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import com.stumbleupon.async.Deferred;

public class PutAction extends Action<Void> {
	
	private final PutRequest put;

	public PutAction(PutRequest put) {
		super();
		this.put = put;
	}

	public PutRequest getPut() {
		return put;
	}
	
	@Override
	public Deferred<Void> perform(HBaseClient client) throws Exception {
		client.put(this.getPut());
		return null;
		
		// pourquoi à ce niveau on ne rajoute pas de callback pour avoir le resultat????
	}
	
}
