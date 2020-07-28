package personal.wqm.springbootdemo.inspur.relationaldb.factory;

public class OracleFactory implements RelationalDatabaseFactory {
    @Override
    public SqlDialect makeSqlDialect() {
        return new OracleDialect();
    }
}
