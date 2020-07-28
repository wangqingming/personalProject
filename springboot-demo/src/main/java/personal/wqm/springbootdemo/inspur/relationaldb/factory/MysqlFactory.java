package personal.wqm.springbootdemo.inspur.relationaldb.factory;

public class MysqlFactory implements RelationalDatabaseFactory {
    @Override
    public SqlDialect makeSqlDialect() {
        return new MysqlDialect();
    }
}
