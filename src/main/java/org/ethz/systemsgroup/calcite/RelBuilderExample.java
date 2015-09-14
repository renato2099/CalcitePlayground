/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ethz.systemsgroup.calcite;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.test.MockCatalogReader;
import org.apache.calcite.test.MockRelOptPlanner;
import org.apache.calcite.test.MockSqlOperatorTable;
import org.apache.calcite.test.RelBuilderTest;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Util;

/**
 * Example that uses {@link org.apache.calcite.tools.RelBuilder}
 * to create various relational expressions.
 */
public class RelBuilderExample {
    public static boolean enableDecorrelate = true;
    public static boolean enableTrim = false;

    public static void main(String[] args) {
        String sql = "select * from emp where empno = 1";

        final FrameworkConfig config = RelBuilderTest.config().build();
        final RelBuilder builder = RelBuilder.create(config);

        RelRoot relRoot = convertSqlToRel(sql);
        final RelNode node = builder.push(relRoot.project())
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("SAL"), builder.literal(10)))
                .build();

        System.out.println("==========================");
        System.out.println(RelOptUtil.toString(node));
        System.out.println("==========================");
    }

    public static SqlNode parseQuery(String sql) throws Exception {
        SqlParser parser = SqlParser.create(sql);
        SqlNode sqlNode = parser.parseQuery();
        return sqlNode;
    }

    public static RelRoot convertSqlToRel(String sql) {
        Util.pre(sql != null, "sql != null");
        final SqlNode sqlQuery;
        try {
            sqlQuery = parseQuery(sql);
        } catch (Exception e) {
            throw Util.newInternal(e); // todo: better handling
        }
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
//        final Prepare.CatalogReader catalogReader =
//                createCatalogReader(typeFactory);
        final Prepare.CatalogReader catalogReader = new MockCatalogReader(typeFactory, true).init();

//        final SqlValidator validator = createValidator(catalogReader, typeFactory);
        final SqlValidator validator = new FarragoTestValidator(
                createOperatorTable(),
                catalogReader,
                typeFactory,
                SqlConformance.DEFAULT);

        final SqlToRelConverter converter =
                createSqlToRelConverter(
                        validator,
                        catalogReader,
                        typeFactory);
        converter.setTrimUnusedFields(true);
        final SqlNode validatedQuery = validator.validate(sqlQuery);
        RelRoot root =
                converter.convertQuery(validatedQuery, false, true);
        assert root != null;
        if (enableDecorrelate || enableTrim) {
            root = root.withRel(converter.flattenTypes(root.rel, true));
        }
        if (enableDecorrelate) {
            root = root.withRel(converter.decorrelate(sqlQuery, root.rel));
        }
        if (enableTrim) {
            converter.setTrimUnusedFields(true);
            root = root.withRel(converter.trimUnusedFields(false, root.rel));
        }
        return root;
    }

    protected static SqlToRelConverter createSqlToRelConverter(
            final SqlValidator validator,
            final Prepare.CatalogReader catalogReader,
            final RelDataTypeFactory typeFactory) {
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);
        final RelOptCluster cluster =
                RelOptCluster.create(new MockRelOptPlanner(), rexBuilder);
        return new SqlToRelConverter(null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE);
    }

    protected static SqlOperatorTable createOperatorTable() {
        final MockSqlOperatorTable opTab =
                new MockSqlOperatorTable(SqlStdOperatorTable.instance());
        MockSqlOperatorTable.addRamp(opTab);
        return opTab;
    }


    /**
     * Validator for testing.
     */
    private static class FarragoTestValidator extends SqlValidatorImpl {
        public FarragoTestValidator(
                SqlOperatorTable opTab,
                SqlValidatorCatalogReader catalogReader,
                RelDataTypeFactory typeFactory,
                SqlConformance conformance) {
            super(opTab, catalogReader, typeFactory, conformance);
        }

        // override SqlValidator
        public boolean shouldExpandIdentifiers() {
            return true;
        }
    }

}

// End RelBuilderExample.java
