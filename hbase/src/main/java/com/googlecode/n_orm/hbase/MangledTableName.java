package com.googlecode.n_orm.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

public class MangledTableName implements Comparable<MangledTableName> {
	private final String name;
	private byte[] bytes;
	
	public MangledTableName(String name) {
		this.name = this.mangleTableName(name);
	}
	
	public MangledTableName(HTableInterface table) {
		this.bytes = table.getTableName();
		this.name = Bytes.toString(table.getTableName());
		assert this.name.equals(this.mangleTableName(this.name));
	}
	
	public String getName() {
		return name;
	}
	
	public byte[] getNameAsBytes() {
		if (this.bytes == null)
			this.bytes = Bytes.toBytes(this.getName());
		return this.bytes;
	}

	protected String mangleTableName(String table) {
		if (table.startsWith(".") || table.startsWith("-")) {
			table = "t" + table;
		}
		
		for (int i = 0; i < table.length(); i++) {
			char c = table.charAt(i);
			if (! (Character.isLetterOrDigit(c) || c == '_' || c == '-' || c == '.'))
				table = table.substring(0, i) + '_' + table.substring(i+1);
		}
		
		return table;
	}

	@Override
	public String toString() {
		return this.getName();
	}

	@Override
	public int compareTo(MangledTableName o) {
		return this.toString().compareTo(o.toString());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MangledTableName other = (MangledTableName) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}

	public void setAutoFlush(boolean b) {
		// TODO Auto-generated method stub
		
	}

	public void flushCommits() {
		// TODO Auto-generated method stub
		
	}
}