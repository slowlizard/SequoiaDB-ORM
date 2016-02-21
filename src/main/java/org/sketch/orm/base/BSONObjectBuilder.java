package org.sketch.orm.base;

import java.util.Map;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;

public class BSONObjectBuilder {

	private BSONObject bson;

	public BSONObjectBuilder() {

		this.bson = new BasicBSONObject();
	}

	public BSONObjectBuilder addRule(String key, Object value) {

		this.bson.put(key, value);
		return this;
	}

	public BSONObjectBuilder addRule(Map m) {
		this.bson.putAll(m);
		return this;
	}

	/**
	 * or规则 第一个参数为值
	 * 
	 * @param value
	 * @param keys
	 * @return
	 */
	public BSONObjectBuilder addOrRule(Object value, String... keys) {
		BasicBSONList bsonList = new BasicBSONList();
		for (String key : keys) {
			BSONObject arg = new BasicBSONObject();
			arg.put(key, value);
			bsonList.add(arg);
		}
		bson.put(MatchConst._OR, bsonList);
		return this;
	}

	/**
	 * 大于某个字段的值
	 * 
	 * @param value
	 * @param keys
	 * @return
	 */
	public BSONObjectBuilder addGtRule(String key, Object value) {
		BSONObject arg = new BasicBSONObject();
		arg.put(MatchConst._GT, value);
		bson.put(key, arg);
		return this;
	}

	public BSONObject getBson() {
		return this.bson;
	}

	/**
	 * 降序排列
	 * 
	 * @return
	 */
	public static BSONObject getCreateTimeOrderDesc() {
		return new BSONObjectBuilder().addRule("createTime", -1).getBson();
	}

	/**
	 * 升序排列
	 * 
	 * @return
	 */
	public static BSONObject getCreateTimeOrderAsc() {
		return new BSONObjectBuilder().addRule("createTime", 1).getBson();
	}
}