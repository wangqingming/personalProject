package personal.wqm.springbootdemo.inspur.relationaldb.factory;

public class SqlServerFactory implements RelationalDatabaseFactory {
    @Override
    public SqlDialect makeSqlDialect() {
        return new SqlServerDialect();
    }
}
