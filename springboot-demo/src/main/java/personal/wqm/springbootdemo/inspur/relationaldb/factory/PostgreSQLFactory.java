package personal.wqm.springbootdemo.inspur.relationaldb.factory;

public class PostgreSQLFactory implements RelationalDatabaseFactory {
    @Override
    public SqlDialect makeSqlDialect() {
        return new PostgreSQLDialect();
    }
}
