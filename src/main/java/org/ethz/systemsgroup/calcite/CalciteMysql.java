package org.ethz.systemsgroup.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;

/**
 * Simple example of using Calcite over a Mysql dataset
 */
public class CalciteMysql {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // creates a calcite driver so queries go through it
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);

        // creating mysql connection
        Class.forName("com.mysql.jdbc.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:mysql://localhost");
        dataSource.setUsername("root");
        dataSource.setPassword("root");
        dataSource.setDefaultCatalog("test");
        JdbcSchema jdbcSchema = JdbcSchema.create(calciteConnection.getRootSchema(), "test", dataSource, null, "test");

        // adding schema to connection
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("test", jdbcSchema);

        // creating statement and executing
        Statement statement = connection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select *\n"
                        + "from \"test\".\"cities\"");
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            int n = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= n; i++) {
                buf.append(i > 1 ? "; " : "")
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getObject(i));
            }
            System.out.println(buf.toString());
            buf.setLength(0);
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
