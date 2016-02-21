package org.sketch.orm.base;

import com.sequoiadb.base.CollectionSpace;
import com.sequoiadb.base.DBCollection;
import com.sequoiadb.base.DBCursor;
import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.exception.BaseException;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.util.ArrayList;
import java.util.List;

public class SequoiaDataAccess {

	protected String dataSpaceName;
	protected String dataCollectionName;

	/**
	 * 查询一条数据
	 * 
	 * @param matcher
	 * @param selector
	 * @param order
	 * @return
	 */
	public BSONObject queryOne(BSONObject matcher, BSONObject selector, BSONObject order) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			return getDbCollection(sdb).queryOne(matcher, selector, order, null, 0);
		} catch (Exception e) {

			e.printStackTrace();
		} finally {

			DbConnectionPool.free(sdb);

		}
		return null;
	}

	/**
	 * 查询所有数据
	 * 
	 * @param dbcollection
	 * @param matcher
	 * @param selector
	 * @param order
	 * @return
	 */
	public List<BSONObject> query(BSONObject matcher, BSONObject selector, BSONObject order) {
		return this.query(matcher, selector, order, 0, -1);
	}

	/**
	 * 
	 * @param dbcollection
	 * @return
	 */
	public List<BSONObject> queryAll() {
		List<BSONObject> list = new ArrayList<BSONObject>();
		DBCursor cursor = null;
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			cursor = getDbCollection(sdb).query();
			while (cursor.hasNext()) {
				BSONObject record = cursor.getNext();
				list.add(record);
			}
		} catch (BaseException e) {

			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
		return list;
	}

	/**
	 * 
	 * 查询多少条数据
	 * 
	 * @param matcher
	 * @param selector
	 * @param order
	 * @param skipRows
	 * @param returnRows
	 * @return
	 */
	public List<BSONObject> query(BSONObject matcher, BSONObject selector, BSONObject order, long skipRows,
			long returnRows) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		List<BSONObject> list = new ArrayList<BSONObject>();
		DBCursor cursor = null;
		try {
			cursor = getDbCollection(sdb).query(matcher, selector, order, null, skipRows, returnRows);
			while (cursor.hasNext()) {
				BSONObject record = cursor.getNext();
				list.add(record);
			}
		} catch (BaseException e) {

			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
		return list;
	}

	/**
	 * 
	 * @param matcher
	 * @param obj
	 */
	public void updateUsingSetOpreator(BSONObject matcher, Object obj) {
		BSONObject modifier = new BasicBSONObject();
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			modifier.put("$set", BasicBSONObject.typeToBson(obj));
			getDbCollection(sdb).update(matcher, modifier, null);
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
	}

	/**
	 * 
	 * @param dbcollection
	 * @param matcher
	 * @param record
	 */
	public void updateWithUserDefine(BSONObject matcher, Object record) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			BSONObject modifier = BasicBSONObject.typeToBson(record);
			getDbCollection(sdb).update(matcher, modifier, null);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
	}
	
	/**
	 * 
	 * @param matcher
	 * @param fieldName
	 * @param increment
	 */
	public void updateCounter(BSONObject matcher, String fieldName, int increment) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			BSONObject obj = new BasicBSONObject();
			BSONObject rule = new BasicBSONObject();
			rule.put(fieldName, increment);
			obj.put(MatchConst._INC, rule);
			getDbCollection(sdb).update(matcher, obj, null);
		} catch (BaseException e) {
			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
	}

	public void delete(BSONObject matcher) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			getDbCollection(sdb).delete(matcher);
		} catch (BaseException e) {

			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
	}

	public void insert(Object obj) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		if (obj != null) {
			try {
				BSONObject bson = BasicBSONObject.typeToBson(obj);
				getDbCollection(sdb).insert(bson);
			} catch (Exception e) {

				e.printStackTrace();
			} finally {
				DbConnectionPool.free(sdb);
			}
		}
	}

	/**
	 * 
	 * @param items
	 */
	public void batchInsert(List<BSONObject> items) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			getDbCollection(sdb).bulkInsert(items, 0);
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
	}

	/**
	 * @param sdb
	 * @return
	 */
	private DBCollection getDbCollection(Sequoiadb sdb) {
		CollectionSpace db = null;
		DBCollection dbCollection = null;
		if (sdb.isCollectionSpaceExist(dataSpaceName))
			db = sdb.getCollectionSpace(dataSpaceName);
		else
			db = sdb.createCollectionSpace(dataSpaceName);
		if (db.isCollectionExist(dataCollectionName))
			dbCollection = db.getCollection(dataCollectionName);
		else
			dbCollection = db.createCollection(dataCollectionName);
		return dbCollection;
	}

	public long getCount(BSONObject matcher) {
		Sequoiadb sdb = DbConnectionPool.getConnection();
		try {
			return getDbCollection(sdb).getCount(matcher);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbConnectionPool.free(sdb);
		}
		return 0;
	}
}
