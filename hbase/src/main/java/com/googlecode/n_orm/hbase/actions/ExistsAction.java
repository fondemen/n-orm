package com.googlecode.n_orm.hbase.actions;

import java.io.IOException;


//import org.apache.hadoop.hbase.client.Get;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;

import com.stumbleupon.async.Deferred;

/*
 * Verifie si le get retourne des éléments ou pas
 */
public class ExistsAction extends Action<Object> {
	
	private final GetRequest get;

	public ExistsAction(GetRequest get) {
		super();
		this.get = get;
	}

	public GetRequest getGet() {
		return get;
	}

	@Override
	public Deferred<Object> perform(HBaseClient client) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
}