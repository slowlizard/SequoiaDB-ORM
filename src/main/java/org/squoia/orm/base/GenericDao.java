package org.squoia.orm.base;

import java.beans.IntrospectionException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.sequoia.orm.annotation.Collection;
import org.sequoia.orm.annotation.PrimaryKey;
import org.sequoia.orm.annotation.Space;



public abstract class GenericDao<T> extends SequoiaDataAccess {
	public static int DEFAULT_PAGE_SIZE = 20;
	private String primaryKey;
	private Class<T> entityClass;

	@SuppressWarnings("unchecked")
	@PostConstruct
	public void init() {
		Type genType = getClass().getGenericSuperclass();
		Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
		entityClass = (Class<T>) params[0];
		Space space = entityClass.getAnnotation(Space.class);
		if (space != null) {
			this.dataSpaceName = space.value();
		}
		Collection collectionName = entityClass.getAnnotation(Collection.class);
		if (collectionName != null) {
			this.dataCollectionName = collectionName.value();
		}

		PrimaryKey primaryKey = entityClass.getAnnotation(PrimaryKey.class);
		if (primaryKey != null) {
			this.primaryKey = primaryKey.value();
		}

	}

	protected void beforeInsert(T obj) {

	}

	protected void afterInsert(T obj) {

	}

	/**
	 * 查询符合某个属性的所有记录
	 * 
	 * @param field
	 * @param value
	 * @param order
	 * @return
	 */
	public List<T> finaAllByAttribute(String field, String value, BSONObject order) {

		BSONObject matcher = new BasicBSONObject();
		matcher.put(field, value);
		return findAll(matcher, order);

	}

	/**
	 * 查询按照某个排序的单条记录
	 * 
	 * @param matcher
	 * @param orderBy
	 * @return
	 */
	public T findOne(BSONObject matcher, BSONObject orderBy) {
		BSONObject obj = queryOne(matcher, null, orderBy);
		try {
			if (obj != null) {
				return obj.as(entityClass);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param matcher
	 * @return
	 */
	public T findOne(BSONObject matcher) {
		return findOne(matcher, null);
	}

	/**
	 * 
	 * @param matcher
	 * @param clazz
	 * @param orderBy
	 * @return
	 */
	public <V> V findOne(BSONObject matcher, Class<V> clazz, BSONObject orderBy) {
		BSONObject obj = queryOne(matcher, null, orderBy);
		try {
			return obj.as(clazz);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param matcher
	 * @param selector
	 * @param orderBy
	 * @return
	 */
	public T findOne(BSONObject matcher, BSONObject selector, BSONObject orderBy) {
		return findOne(matcher, selector, orderBy);
	}

	/**
	 * @param matcher
	 * @param selector
	 * @param orderBy
	 * @param page
	 * @return
	 */
	private List<BSONObject> queryPerPage(BSONObject matcher, BSONObject selector, BSONObject orderBy, Integer page) {
		int pageSize = Integer.valueOf(page);
		long skip = DEFAULT_PAGE_SIZE * pageSize;
		return query(matcher, selector, orderBy, skip, DEFAULT_PAGE_SIZE);
	}

	/*
	*
	*/
	public List<T> findPerPage(BSONObject matcher, BSONObject orderBy, Integer page) {
		try {
			List<BSONObject> result = this.queryPerPage(matcher, null, orderBy, page);
			List<T> result1 = new ArrayList<T>();
			for (BSONObject bsonObject : result) {
				result1.add(bsonObject.as(entityClass));
			}
			return result1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public <V> List<V> findPerPage(BSONObject matcher, BSONObject orderBy, Class<V> clazz, Integer page) {
		try {
			BSONObject selector = BasicBSONObject.typeToBson(clazz.newInstance());
			List<BSONObject> result = this.queryPerPage(matcher, selector, orderBy, page);
			List<V> result1 = new ArrayList<V>();
			for (BSONObject bsonObject : result) {
				result1.add(bsonObject.as(clazz));
			}
			return result1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param matcher
	 * @param orderBy
	 * @param count
	 * @return List<T>
	 */
	public List<T> findByGivenCount(BSONObject matcher, BSONObject orderBy, int count) {
		List<T> list = new ArrayList<T>();
		try {

			List<BSONObject> result = query(matcher, null, orderBy, 0, count);
			for (BSONObject bsonObject : result) {
				list.add(bsonObject.as(entityClass));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}
	

	/**
	 * @param matcher
	 * @param orderBy
	 * @param clazz
	 * @param count
	 * @return
	 */
	public <V> List<V> findAll(BSONObject matcher, BSONObject orderBy, Class<V> clazz, int count) {
		List<V> list = new ArrayList<V>();
		try {
			BSONObject selector = BasicBSONObject.typeToBson(clazz.newInstance());
			List<BSONObject> result = query(matcher, selector, orderBy, 0, count);
			for (BSONObject bsonObject : result) {
				list.add(bsonObject.as(clazz));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public <V> List<V> findAll(BSONObject matcher, Class<V> clazz, BSONObject orderBy) {
		List<V> list = new ArrayList<V>();
		try {
			BSONObject selector = BasicBSONObject.typeToBson(clazz.newInstance());
			List<BSONObject> result = query(matcher, selector, orderBy);
			for (BSONObject bsonObject : result) {
				list.add(bsonObject.as(clazz));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	public List<T> findAll(BSONObject matcher, BSONObject orderBy) {
		List<T> result1 = new ArrayList<T>();
		if (matcher == null || matcher.toMap().size() == 0) {
			return result1;
		}
		try {
			List<BSONObject> result = query(matcher, null, orderBy);

			for (BSONObject bsonObject : result) {
				result1.add(bsonObject.as(entityClass));
			}
			return result1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result1;
	}

	public List<T> findAll(BSONObject matcher, BSONObject selector, BSONObject order) {
		try {
			List<BSONObject> result = query(matcher, selector, order);
			List<T> result1 = new ArrayList<T>();
			for (BSONObject bsonObject : result) {
				result1.add(bsonObject.as(entityClass));
			}
			return result1;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 如果count字段为空，怎么办
	 *
	 * @param primaryKeyValue
	 * @param field
	 */
	public void incrementCounterByPk(String primaryKeyValue, String field) {
		BSONObject matcher = new BasicBSONObject();
		matcher.put(this.primaryKey, primaryKeyValue);
		this.incrementCounter(matcher, field);
	}

	public void incrementCounter(BSONObject matcher, String field) {
		BSONObject obj = new BasicBSONObject();
		BSONObject arg1 = new BasicBSONObject();
		arg1.put(field, 1);
		obj.put(MatchConst._INC, arg1);
		super.updateWithUserDefine(matcher, obj);
	}

	public void decrementCounterByPk(String primaryKeyValue, String field) {
		BSONObject matcher = new BasicBSONObject();
		matcher.put(this.primaryKey, primaryKeyValue);
		this.decrementCounter(matcher, field);
	}

	public void decrementCounter(BSONObject matcher, String field) {
		BSONObject obj = new BasicBSONObject();
		BSONObject arg1 = new BasicBSONObject();
		arg1.put(field, -1);
		obj.put(MatchConst._INC, arg1);
		super.updateWithUserDefine(matcher, obj);
	}

	public void pull(String primaryKeyValue, String field, String value) {
		BSONObject matcher = new BasicBSONObject();
		matcher.put(this.primaryKey, primaryKeyValue);
		BSONObject obj = new BasicBSONObject();
		BSONObject arg1 = new BasicBSONObject();
		arg1.put(field, value);
		obj.put(MatchConst._PULL, arg1);
		matcher.put(this.primaryKey, primaryKeyValue);
		super.updateWithUserDefine(matcher, obj);
	}

	public void push(String primaryKeyValue, String field, String value) {
		BSONObject matcher = new BasicBSONObject();
		matcher.put(this.primaryKey, primaryKeyValue);
		BSONObject obj = new BasicBSONObject();
		BSONObject arg1 = new BasicBSONObject();
		arg1.put(field, value);
		obj.put(MatchConst._PUSH, arg1);
		matcher.put(this.primaryKey, primaryKeyValue);
		super.updateWithUserDefine(matcher, obj);
	}

	public synchronized void generateId(T obj) {
		String id = null;
		while (true) {
			BSONObject matcher = new BasicBSONObject();
			id = CommEncode.generateId();
			matcher.put(this.primaryKey, id);
			BSONObject bson = queryOne(matcher, null, null);
			if (bson == null) {
				break;
			}
		}
		try {
			Class cls = obj.getClass();
			Method method = cls.getMethod(this.getGenerateIdKeySetMethodName(), String.class);
			method.invoke(obj, id);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * @param obj
	 * @param isGenerateId
	 * @return
	 */
	public synchronized T insert(T obj, boolean isGenerateId) {

		if (isGenerateId) {
			this.generateId(obj);
		}
		this.insert(obj);

		return obj;
	}

	@Override
	public void insert(Object obj) {

		beforeInsert((T) obj);
		super.insert(obj);
		afterInsert((T) obj);
	}

	/**
	 * 批量保存
	 *
	 * @param items
	 * @param <T>
	 * @see
	 * 		<p>
	 *      使用此接口请保证上层ID生成与保存同步
	 *      </p>
	 */
	public <T> void batchSave(List<T> items) {
		if (items.size() == 0) {
			return;
		}
		List<BSONObject> bsonItems = new ArrayList<BSONObject>();
		try {
			for (T item : items) {
				BSONObject bsonObject = BasicBSONObject.typeToBson(item);
				bsonItems.add(bsonObject);
			}
			batchInsert(bsonItems);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IntrospectionException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	public void delete(BSONObject matcher) {

		super.delete(matcher);

	}

	public List<BSONObject> search(String[] args, String value, BSONObject selector) {
		BSONObject matcher = new BasicBSONObject();
		BasicBSONList bsonList = new BasicBSONList();
		for (String key : args) {
			BSONObject arg = new BasicBSONObject();
			// BSONObject regex = new BasicBSONObject();
			// regex.put(MatchConst._REGEX,value);
			arg.put(key, value);
			bsonList.add(arg);
		}
		matcher.put(MatchConst._OR, bsonList);
		return query(matcher, selector, null);

	}

	public List<T> search(String key, String value, BSONObject order) {
		BSONObject arg = new BasicBSONObject();
		BSONObject regex = new BasicBSONObject();
		regex.put(MatchConst._REGEX, value);
		arg.put(key, regex);
		List<BSONObject> record = query(arg, null, null);
		List<T> result1 = new ArrayList<T>();
		for (BSONObject bsonObject : record) {
			try {
				result1.add(bsonObject.as(entityClass));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result1;
	}
	public <V> List<V> search(String key, String value, BSONObject order,Class<V> clazz) {
		BSONObject arg = new BasicBSONObject();
		BSONObject regex = new BasicBSONObject();
		regex.put(MatchConst._REGEX, value);
		arg.put(key, regex);
		List<BSONObject> record = query(arg, null, null);
		List<V> result1 = new ArrayList<V>();
		for (BSONObject bsonObject : record) {
			try {
				result1.add(bsonObject.as(clazz));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result1;
	}


	public int getPageCount(BSONObject matcher) {
		long count = getCount(matcher);
		if (count % DEFAULT_PAGE_SIZE == 0) {
			return (int) (count / DEFAULT_PAGE_SIZE);
		} else {
			return (int) ((count / DEFAULT_PAGE_SIZE) + 1);
		}
	}

	public int getRecordCount(BSONObject matcher) {
		return (int) getCount(matcher);
	}

	public T findOneByAttribute(String field, String value) {
		BSONObject matcher = new BasicBSONObject();
		matcher.put(field, value);
		return this.findOne(matcher, null);
	}

	protected <T> List<T> caseBsonsToList(List<BSONObject> targetList, Class<T> cls) {
		List<T> result = new ArrayList<T>();
		for (BSONObject bsonObject : targetList) {
			try {
				T target = bsonObject.as(cls);
				result.add(target);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	public boolean update(T t) {
		BSONObject query = new BasicBSONObject();
		Class<?> cls = t.getClass();
		try {
			Method getIdMethod = cls.getMethod(this.getPrimaryKeyGetMethodName());
			String id = (String) getIdMethod.invoke(t);
			query.put(this.primaryKey, id);
			super.updateUsingSetOpreator(query, t);
			return true;
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 更新一个model的字段的值。会把原来的数据清除掉而不是追加
	 *
	 * @param primaryKeyValue
	 *            主键的值
	 * @param field
	 *            要更新的键
	 * @param value
	 *            要更新的值
	 * @return
	 */
	public boolean updateByPk(String primaryKeyValue, String field, Object value) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		BSONObject update = new BasicBSONObject();
		update.put(field, value);
		try {
			super.updateUsingSetOpreator(query, update);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 通过主键更新
	 * 
	 * @param primaryKeyValue
	 * @param m
	 * @return
	 */
	public boolean updateByPk(String primaryKeyValue, Map m) {
		BSONObject obj = new BSONObjectBuilder().addRule(m).getBson();
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		try {
			super.updateUsingSetOpreator(query, m);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 更新一个数组的字段
	 *
	 * @param primaryKeyValue
	 * @param field
	 * @param value
	 * @return
	 */
	public <T> boolean pushAllByPk(String primaryKeyValue, String field, List<T> value) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		BSONObject update = new BasicBSONObject();
		update.put(field, value);
		update.put(field, value);
		BSONObject modifier = new BasicBSONObject();
		modifier.put("$push_all", update);
		try {
			super.updateWithUserDefine(query, modifier);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public T findOneByPk(String primaryKeyValue) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		return this.findOne(query, null);
	}

	public <V> V findOneByPk(String primaryKeyValue, Class<V> clazz) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		try {
			BSONObject bson = queryOne(query, BasicBSONObject.typeToBson(clazz.newInstance()), null);
			return bson.as(clazz);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<T> findAll() {
		try {
			List<BSONObject> result = queryAll();
			if (result != null && result.size() > 0) {
				return this.caseBsonsToList(result, entityClass);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * 取出来的值要按照主键ID的顺序
	 *
	 * @param primaryKeyValues
	 * @param cls
	 * @return
	 */
	public List<T> findListByPkArray(List<String> primaryKeyValues) {
		BSONObject selector = null;
		BSONObject query = new BasicBSONObject();
		BSONObject matcher = new BasicBSONObject();
		matcher.put(MatchConst._IN, primaryKeyValues);
		query.put(this.primaryKey, matcher);
		try {
			List<BSONObject> result = query(query, selector, null);
			if (result != null && result.size() > 0) {
				return this.caseBsonsToList(result, entityClass);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new ArrayList<>();
	}

	/**
	 * 取出来的值要按照主键ID的顺序
	 *
	 * @param primaryKeyValues
	 * @param cls
	 * @return
	 */
	public List<T> findListByPkArray(String[] primaryKeyValues) {
		BSONObject selector = null;
		BSONObject query = new BasicBSONObject();
		BSONObject matcher = new BasicBSONObject();
		matcher.put(MatchConst._IN, primaryKeyValues);
		query.put(this.primaryKey, matcher);
		try {
			List<BSONObject> result = query(query, selector, null);
			if (result != null && result.size() > 0) {
				return this.caseBsonsToList(result, entityClass);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public <V> List<V> findListByPkArray(List<String> primaryKeyValues, Class<V> clazz) {
		BSONObject query = new BasicBSONObject();
		BSONObject matcher = new BasicBSONObject();
		matcher.put(MatchConst._IN, primaryKeyValues);
		query.put(this.primaryKey, matcher);
		try {
			BSONObject selector = BasicBSONObject.typeToBson(clazz.newInstance());
			List<BSONObject> result = query(query, selector, null);
			if (result != null && result.size() > 0) {
				return this.caseBsonsToList(result, clazz);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @param primaryKeyValue
	 * @param key
	 * @return
	 */
	public String findAttributeByPk(String primaryKeyValue, String key) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		BSONObject selector = new BasicBSONObject();
		selector.put(key, key);
		BSONObject bsonObejct = queryOne(query, selector, null);
		if (bsonObejct != null) {
			return (String) bsonObejct.get(key);
		}
		return null;
	}

	public <V> V findAttributeByPk(String primaryKeyValue, Class<V> clazz) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, primaryKeyValue);
		BSONObject selector;
		try {
			selector = BasicBSONObject.typeToBson(clazz.newInstance());
			BSONObject bsonObejct = queryOne(query, selector, null);
			if (bsonObejct != null) {
				return bsonObejct.as(clazz);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	public boolean deleteAll(String[] ids) {
		BSONObject query = new BasicBSONObject();
		BSONObject matcher = new BasicBSONObject();
		matcher.put(MatchConst._IN, ids);
		query.put(this.primaryKey, matcher);
		try {
			this.delete(query);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean deleteAll(T[] targets) {
		if (targets.length < 1)
			return false;
		String[] ids = new String[targets.length];
		Class cls = targets[0].getClass();
		try {
			Method method = cls.getMethod(this.getPrimaryKeyGetMethodName());
			for (int i = 0; i < ids.length; i++) {
				T target = targets[i];
				String id = (String) method.invoke(target);
				ids[i] = id;
			}
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return deleteAll(ids);
	}

	/**
	 * 删除某个对象
	 * 
	 * @param object
	 * @return
	 */
	public boolean delete(T object) {
		BSONObject matcher = null;
		try {
			matcher = BasicBSONObject.typeToBson(object);
			if (matcher != null) {
				beforeDelete(object);
				delete(matcher);
				afterDelete(object);
				return true;
			}
			return false;
		} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException
				| IntrospectionException e) {

			e.printStackTrace();
		}
		return false;
	}

	protected void afterDelete(T object) {

	}

	protected void beforeDelete(T object) {

	}

	protected List<BSONObject> aggregate(BSONObject match, BSONObject group, BSONObject project) {
		return aggregate(match, group, project);
	}

	public boolean deleteByPk(String id) {
		BSONObject query = new BasicBSONObject();
		query.put(this.primaryKey, id);
		try {
			this.delete(query);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public long count(BSONObject query) {
		return getCount(query);
	}

	/**
	 * @param keywords
	 * @param field
	 * @param selector
	 * @param order
	 * @return
	 */
	protected List<BSONObject> search(String keywords, String field, String[] selector, BSONObject order,
			boolean isMatchStrict) {
		int length = selector.length;
		BSONObject selectorBson = new BasicBSONObject();
		for (int i = 0; i < length; i++) {
			selectorBson.put(selector[i], selector[i]);
		}
		if (keywords != null && !keywords.isEmpty()) {
			BSONObject matcherOne = new BasicBSONObject();
			BasicBSONList arr = new BasicBSONList();
			BSONObject parmasOne = new BasicBSONObject();
			matcherOne.put(MatchConst._REGEX, keywords);
			matcherOne.put(MatchConst._OPTIONS, "i");
			parmasOne.put("pinyin", matcherOne);
			arr.add(parmasOne);
			BSONObject parmasTwo = new BasicBSONObject();
			BSONObject matcherTwo = new BasicBSONObject();
			matcherTwo.put(MatchConst._REGEX, keywords);
			if (!isMatchStrict) {
				parmasTwo.put(field, matcherTwo);
			} else {
				parmasTwo.put(field, keywords);
			}
			arr.add(parmasTwo);
			BSONObject query = new BasicBSONObject();
			query.put(MatchConst._OR, arr);
			List<BSONObject> record = query(query, null, order);
			return record;
		}
		return null;
	}

	private String getGenerateIdKeySetMethodName() {
		return "set" + this.primaryKey.substring(0, 1).toUpperCase() + this.primaryKey.substring(1);
	}

	private String getGenerateIdKeyGetMethodName() {
		return "get" + this.primaryKey.substring(0, 1).toUpperCase() + this.primaryKey.substring(1);
	}

	private String getPrimaryKeyGetMethodName() {
		return "get" + this.primaryKey.substring(0, 1).toUpperCase() + this.primaryKey.substring(1);
	}

}
