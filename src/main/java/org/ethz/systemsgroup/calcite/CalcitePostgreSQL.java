package org.ethz.systemsgroup.calcite;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.commons.dbcp.BasicDataSource;

import java.sql.*;

/**
 * Created by kentsay on 7/6/15.
 */
public class CalcitePostgreSQL {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // creates a calcite driver so queries go through it
        Class.forName("org.apache.calcite.jdbc.Driver");
        Connection connection =
                DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection =
                connection.unwrap(CalciteConnection.class);

        // creating mysql connection
        Class.forName("org.postgresql.Driver");
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl("jdbc:postgresql://127.0.0.1/demo");

        dataSource.setUsername("kentsay");
        dataSource.setPassword("kentsay");
        dataSource.setDefaultCatalog("demo");

        // The connection is completely empty until JdbcSchema.create registers a Java object as a schema and its collection test as tables
        JdbcSchema jdbcSchema = JdbcSchema.create(calciteConnection.getRootSchema(), "demo", dataSource, null, "demo");

        // By list table name from jdbcSchema interface, you can make sure what table you have in postgresql
        for(String table: jdbcSchema.getTableNames()) {
            System.out.println(table);
        }
        // adding schema to connection
        calciteConnection.getRootSchema().add("demo", jdbcSchema);

        // creating statement and executing
        Statement statement = calciteConnection.createStatement();
        ResultSet resultSet =
                statement.executeQuery("select *\n"
                        + "from \"demo\".\"tenants\"");
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