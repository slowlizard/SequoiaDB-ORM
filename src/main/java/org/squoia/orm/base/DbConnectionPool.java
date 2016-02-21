package org.squoia.orm.base;


import com.sequoiadb.base.Sequoiadb;
import com.sequoiadb.base.SequoiadbDatasource;
import com.sequoiadb.exception.BaseException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Properties;

public class DbConnectionPool {
    private static SequoiadbDatasource datasource = null;
    static {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("dbconfig.xml");
        datasource = (SequoiadbDatasource) applicationContext.getBean("dataSource");
    }
    public static Sequoiadb getConnection() {
        try {
            return datasource.getConnection();
        } catch (BaseException e) {
          
            e.printStackTrace();
        } catch (InterruptedException e) {
          
            e.printStackTrace();
        }
        return null;
    }

    public static void free(Sequoiadb sdb) {
        if (sdb != null && sdb.isValid())
            datasource.close(sdb);
    }
}
