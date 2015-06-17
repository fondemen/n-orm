package com.googlecode.n_orm.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;

public class MangledTableName implements Comparable<MangledTableName> {

	protected static String mangleTableName(String table) {
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
	
	private TableName tableName;
	
	public MangledTableName(String name) {
		this.tableName = TableName.valueOf(mangleTableName(name));
	}
	
	public MangledTableName(Table table) {
		this.tableName = table.getName();
		assert this.getName().equals(mangleTableName(this.getName()));
	}
	
	public String getName() {
		return this.tableName.getNameAsString();
	}
	
	public byte[] getNameAsBytes() {
		return this.tableName.getName();
	}
	
	public TableName getTableName() {
		return this.tableName;
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
		result = prime * result + ((this.getName() == null) ? 0 : this.getName().hashCode());
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
		String thisName = this.getName();
		String otherName = other.getName();
		if (thisName == null) {
			if (otherName != null)
				return false;
		} else if (!thisName.equals(otherName))
			return false;
		return true;
	}
}