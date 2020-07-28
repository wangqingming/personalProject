package personal.wqm.springbootdemo.inspur.relationaldb;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.expression.operators.relational.NamedExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

public class SqlParserDemo {

    public static void main(String[] args) throws Exception {

        createTable();
    }


    public static void createTable() throws Exception {
        Insert insert = (Insert) CCJSqlParserUtil.parse("insert into mytable (col1) values (1)");
        System.out.println(insert.toString());

        //adding another column
        insert.getColumns().add(new Column("col3"));
        //adding another value (the easy way)

        System.out.println(insert.toString());
    }


}
