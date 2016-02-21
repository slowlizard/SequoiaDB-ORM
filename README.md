# Sketch
巨杉数据库(SequoiaDB)的ORM框架，未来希望能够直接无缝兼容mongoDB。由于时间仓促，很多地方没来的优化，希望各位谅解。
这个框架既可以直接用于生产环境，也可以参照里面的实现自己实现ORM。
假设在使用过程中遇到问题，可以直接加QQ：741691740或者发邮件给我：741691740@qq.com
使用：
##修改dbconfig.xml配置，改成自己的巨杉数据库的配置。
然后继承GenericDao<T>，比如：
```java
class UserDao extends GenericDao<User>{

}
```
直接使用注解来为实体设置SpaceName，CollectionName以及PrimaryKey。

