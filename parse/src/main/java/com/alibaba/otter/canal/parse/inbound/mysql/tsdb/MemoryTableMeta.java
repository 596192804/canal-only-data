package com.alibaba.otter.canal.parse.inbound.mysql.tsdb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastsql.sql.ast.SQLDataType;
import com.alibaba.fastsql.sql.ast.SQLDataTypeImpl;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLCharExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLNullExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnConstraint;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnPrimaryKey;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnUniqueKey;
import com.alibaba.fastsql.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.fastsql.sql.ast.statement.SQLNullConstraint;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.fastsql.sql.ast.statement.SQLTableElement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.fastsql.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.fastsql.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.fastsql.sql.repository.Schema;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import com.alibaba.fastsql.util.JdbcConstants;
import com.alibaba.otter.canal.parse.inbound.TableMeta;
import com.alibaba.otter.canal.parse.inbound.TableMeta.FieldMeta;
import com.alibaba.otter.canal.parse.inbound.mysql.ddl.DruidDdlParser;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

/**
 * ??????DDL????????????????????????
 *
 * @author agapple 2017???7???27??? ??????4:19:40
 * @since 3.2.5
 */
public class MemoryTableMeta implements TableMetaTSDB {

    private Logger                       logger     = LoggerFactory.getLogger(MemoryTableMeta.class);
    private Map<List<String>, TableMeta> tableMetas = new ConcurrentHashMap<List<String>, TableMeta>();
    private SchemaRepository             repository = new SchemaRepository(JdbcConstants.MYSQL);

    public MemoryTableMeta(){
    }

    @Override
    public boolean init(String destination) {
        return true;
    }

    @Override
    public void destory() {
        tableMetas.clear();
    }

    public boolean apply(EntryPosition position, String schema, String ddl, String extra) {
        tableMetas.clear();
        synchronized (this) {
            if (StringUtils.isNotEmpty(schema)) {
                repository.setDefaultSchema(schema);
            }

            try {
                // druid??????flush privileges?????????????????????
                if (!StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "flush")
                    && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "grant")
                    && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "revoke")
                    && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "create user")
                    && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "alter user")
                    && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "drop user")
                    && !StringUtils.startsWithIgnoreCase(StringUtils.trim(ddl), "create database")) {
                    repository.console(ddl);
                }
            } catch (Throwable e) {
                logger.warn("parse faield : " + ddl, e);
            }
        }

        // TableMeta meta = find("tddl5_00", "ab");
        // if (meta != null) {
        // repository.setDefaultSchema("tddl5_00");
        // System.out.println(repository.console("show create table tddl5_00.ab"));
        // System.out.println(repository.console("show columns from tddl5_00.ab"));
        // }
        return true;
    }

    @Override
    public TableMeta find(String schema, String table) {
        List<String> keys = Arrays.asList(schema, table);
        TableMeta tableMeta = tableMetas.get(keys);
        if (tableMeta == null) {
            synchronized (this) {
                tableMeta = tableMetas.get(keys);
                if (tableMeta == null) {
                    Schema schemaRep = repository.findSchema(schema);
                    if (schemaRep == null) {
                        return null;
                    }
                    SchemaObject data = schemaRep.findTable(table);
                    if (data == null) {
                        return null;
                    }
                    SQLStatement statement = data.getStatement();
                    if (statement == null) {
                        return null;
                    }
                    if (statement instanceof SQLCreateTableStatement) {
                        tableMeta = parse((SQLCreateTableStatement) statement);
                    }
                    if (tableMeta != null) {
                        if (table != null) {
                            tableMeta.setTable(table);
                        }
                        if (schema != null) {
                            tableMeta.setSchema(schema);
                        }

                        tableMetas.put(keys, tableMeta);
                    }
                }
            }
        }

        return tableMeta;
    }

    @Override
    public boolean rollback(EntryPosition position) {
        throw new RuntimeException("not support for memory");
    }

    public Map<String, String> snapshot() {
        Map<String, String> schemaDdls = new HashMap<String, String>();
        for (Schema schema : repository.getSchemas()) {
            StringBuffer data = new StringBuffer(4 * 1024);
            for (String table : schema.showTables()) {
                SchemaObject schemaObject = schema.findTable(table);
                schemaObject.getStatement().output(data);
                data.append("; \n");
            }
            schemaDdls.put(schema.getName(), data.toString());
        }

        return schemaDdls;
    }

    private TableMeta parse(SQLCreateTableStatement statement) {
        int size = statement.getTableElementList().size();
        if (size > 0) {
            TableMeta tableMeta = new TableMeta();
            for (int i = 0; i < size; ++i) {
                SQLTableElement element = statement.getTableElementList().get(i);
                processTableElement(element, tableMeta);
            }
            return tableMeta;
        }

        return null;
    }

    private void processTableElement(SQLTableElement element, TableMeta tableMeta) {
        if (element instanceof SQLColumnDefinition) {
            FieldMeta fieldMeta = new FieldMeta();
            SQLColumnDefinition column = (SQLColumnDefinition) element;
            String name = getSqlName(column.getName());
            // String charset = getSqlName(column.getCharsetExpr());
            SQLDataType dataType = column.getDataType();
            String dataTypStr = dataType.getName();
            if (StringUtils.equalsIgnoreCase(dataTypStr, "float")) {
                if (dataType.getArguments().size() == 1) {
                    int num = Integer.valueOf(dataType.getArguments().get(0).toString());
                    if (num > 24) {
                        dataTypStr = "double";
                    }
                }
            }

            if (dataType.getArguments().size() > 0) {
                dataTypStr += "(";
                for (int i = 0; i < column.getDataType().getArguments().size(); i++) {
                    if (i != 0) {
                        dataTypStr += ",";
                    }
                    SQLExpr arg = column.getDataType().getArguments().get(i);
                    dataTypStr += arg.toString();
                }
                dataTypStr += ")";
            }

            if (dataType instanceof SQLDataTypeImpl) {
                SQLDataTypeImpl dataTypeImpl = (SQLDataTypeImpl) dataType;
                if (dataTypeImpl.isUnsigned()) {
                    dataTypStr += " unsigned";
                }

                if (dataTypeImpl.isZerofill()) {
                    // mysql default behaiver
                    // ???????????????zerofill?????????????????????unsigned??????
                    if (!dataTypeImpl.isUnsigned()) {
                        dataTypStr += " unsigned";
                    }

                    dataTypStr += " zerofill";
                }
            }

            if (column.getDefaultExpr() == null || column.getDefaultExpr() instanceof SQLNullExpr) {
                fieldMeta.setDefaultValue(null);
            } else {
                fieldMeta.setDefaultValue(DruidDdlParser.unescapeQuotaName(getSqlName(column.getDefaultExpr())));
            }

            fieldMeta.setColumnName(name);
            fieldMeta.setColumnType(dataTypStr);
            fieldMeta.setNullable(true);
            List<SQLColumnConstraint> constraints = column.getConstraints();
            for (SQLColumnConstraint constraint : constraints) {
                if (constraint instanceof SQLNotNullConstraint) {
                    fieldMeta.setNullable(false);
                } else if (constraint instanceof SQLNullConstraint) {
                    fieldMeta.setNullable(true);
                } else if (constraint instanceof SQLColumnPrimaryKey) {
                    fieldMeta.setKey(true);
                    fieldMeta.setNullable(false);
                } else if (constraint instanceof SQLColumnUniqueKey) {
                    fieldMeta.setUnique(true);
                }
            }
            tableMeta.addFieldMeta(fieldMeta);
        } else if (element instanceof MySqlPrimaryKey) {
            MySqlPrimaryKey column = (MySqlPrimaryKey) element;
            List<SQLSelectOrderByItem> pks = column.getColumns();
            for (SQLSelectOrderByItem pk : pks) {
                String name = getSqlName(pk.getExpr());
                FieldMeta field = tableMeta.getFieldMetaByName(name);
                field.setKey(true);
                field.setNullable(false);
            }
        } else if (element instanceof MySqlUnique) {
            MySqlUnique column = (MySqlUnique) element;
            List<SQLSelectOrderByItem> uks = column.getColumns();
            for (SQLSelectOrderByItem uk : uks) {
                String name = getSqlName(uk.getExpr());
                FieldMeta field = tableMeta.getFieldMetaByName(name);
                field.setUnique(true);
            }
        }
    }

    private String getSqlName(SQLExpr sqlName) {
        if (sqlName == null) {
            return null;
        }

        if (sqlName instanceof SQLPropertyExpr) {
            SQLIdentifierExpr owner = (SQLIdentifierExpr) ((SQLPropertyExpr) sqlName).getOwner();
            return DruidDdlParser.unescapeName(owner.getName()) + "."
                   + DruidDdlParser.unescapeName(((SQLPropertyExpr) sqlName).getName());
        } else if (sqlName instanceof SQLIdentifierExpr) {
            return DruidDdlParser.unescapeName(((SQLIdentifierExpr) sqlName).getName());
        } else if (sqlName instanceof SQLCharExpr) {
            return ((SQLCharExpr) sqlName).getText();
        } else if (sqlName instanceof SQLMethodInvokeExpr) {
            return DruidDdlParser.unescapeName(((SQLMethodInvokeExpr) sqlName).getMethodName());
        } else if (sqlName instanceof MySqlOrderingExpr) {
            return getSqlName(((MySqlOrderingExpr) sqlName).getExpr());
        } else {
            return sqlName.toString();
        }
    }

    public SchemaRepository getRepository() {
        return repository;
    }

}
