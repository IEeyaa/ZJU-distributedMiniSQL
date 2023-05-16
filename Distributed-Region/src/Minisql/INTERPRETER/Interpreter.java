package INTERPRETER;

import CATALOGMANAGER.Attribute;
import CATALOGMANAGER.CatalogManager;
import CATALOGMANAGER.NumType;
import CATALOGMANAGER.Table;
import Components.Region;
import INDEXMANAGER.Index;
import INDEXMANAGER.IndexManager;
import RECORDMANAGER.Condition;
import RECORDMANAGER.RecordManager;
import RECORDMANAGER.TableRow;

import java.io.*;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Interpreter {
    static String sql_execute_result = "";

    // Interpreter,输入string内容，返回监听口
    public static String interpret(String result) throws IOException {
        // 持续处理管道输入
        String[] tokens = result.split(" ");
        try {
            if (tokens.length == 1 && tokens[0].equals(""))
                throw new QException(0, 200, "No statement specified");
            switch (tokens[0]) { // match keyword
                case "create":
                    if (tokens.length == 1)
                        throw new QException(0, 201, "Can't find create object");
                    switch (tokens[1]) {
                        case "table":
                            parse_create_table(result);
                            break;
                        case "index":
                            parse_create_index(result);
                            break;
                        default:
                            throw new QException(0, 202, "Can't identify " + tokens[1]);
                    }
                    break;
                case "drop":
                    if (tokens.length == 1)
                        throw new QException(0, 203, "Can't find drop object");
                    switch (tokens[1]) {
                        case "table":
                            parse_drop_table(result);
                            break;
                        case "index":
                            parse_drop_index(result);
                            break;
                        default:
                            throw new QException(0, 204, "Can't identify " + tokens[1]);
                    }
                    break;
                case "select":
                    parse_select(result);
                    break;
                case "insert":
                    parse_insert(result);
                    break;
                case "delete":
                    parse_delete(result);
                    break;
                case "quit":
                    parse_store(result);
                    break;
                case "show":
                    parse_show(result);
                    break;
                default:
                    throw new QException(0, 205, "Can't identify " + tokens[0]);
            }
        } catch (QException e) {
            sql_execute_result = e.status + " " + QException.ex[e.type] + ": " + e.msg;
        } catch (Exception e) {
            sql_execute_result = "Default error: " + e.getMessage();
        }
        return sql_execute_result;
    }

    private static void parse_show(String statement) throws Exception {
        String type = Utils.substring(statement, "show ", "").trim();
        if (type.equals("tables")) {
            sql_execute_result = CatalogManager.show_table();
        } else if (type.equals("indexes")) {
            CatalogManager.show_index();
        } else
            throw new QException(0, 323, "Can not find valid key word after 'show'!");
    }

    private static void parse_create_table(String statement) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^create table", "").trim(); // skip create table keyword

        int startIndex, endIndex;
        if (statement.equals("")) // no statement after create table
            throw new QException(0, 401, "Must specify a table name");

        endIndex = statement.indexOf(" ");
        if (endIndex == -1) // no statement after create table xxx
            throw new QException(0, 402, "Can't find attribute definition");

        String tableName = statement.substring(0, endIndex); // get table name
        startIndex = endIndex + 1; // start index of '('
        if (!statement.substring(startIndex).matches("^\\(.*\\)$")) // check brackets
            throw new QException(0, 403, "Can't not find the definition brackets in table " + tableName);

        int length;
        String[] attrParas, attrsDefine;
        String attrName, attrType, attrLength = "", primaryName = "";
        boolean attrUnique;
        Attribute attribute;
        Vector<Attribute> attrVec = new Vector<>();

        attrsDefine = statement.substring(startIndex + 1).split(","); // get each attribute definition
        for (int i = 0; i < attrsDefine.length; i++) { // for each attribute
            if (i == attrsDefine.length - 1) { // last line
                attrParas = attrsDefine[i].trim().substring(0, attrsDefine[i].length() - 1).split(" "); // remove
                                                                                                        // last
                                                                                                        // ')'
            } else {
                attrParas = attrsDefine[i].trim().split(" ");
            } // split each attribute in parameters: name, type,（length) (unique)

            if (attrParas[0].equals("")) { // empty
                throw new QException(0, 404, "Empty attribute in table " + tableName);
            } else if (attrParas[0].equals("primary")) { // primary key definition
                if (attrParas.length != 3 || !attrParas[1].equals("key")) // not as primary key xxxx
                    throw new QException(0, 405, "Error definition of primary key in table " + tableName);
                if (!attrParas[2].matches("^\\(.*\\)$")) // not as primary key (xxxx)
                    throw new QException(0, 406, "Error definition of primary key in table " + tableName);
                if (!primaryName.equals("")) // already set primary key
                    throw new QException(0, 407, "Redefinition of primary key in table " + tableName);

                primaryName = attrParas[2].substring(1, attrParas[2].length() - 1); // set primary key
            } else { // ordinary definition
                if (attrParas.length == 1) // only attribute name
                    throw new QException(0, 408, "Incomplete definition in attribute " + attrParas[0]);
                attrName = attrParas[0]; // get attribute name
                attrType = attrParas[1]; // get attribute type
                for (int j = 0; j < attrVec.size(); j++) { // check whether name redefines
                    if (attrName.equals(attrVec.get(j).attributeName))
                        throw new QException(0, 409, "Redefinition in attribute " + attrParas[0]);
                }
                if (attrType.equals("int") || attrType.equals("float")) { // check type
                    endIndex = 2; // expected end index
                } else if (attrType.equals("char")) {
                    if (attrParas.length == 2) // no char length
                        throw new QException(0, 410, "ust specify char length in " + attrParas[0]);
                    if (!attrParas[2].matches("^\\(.*\\)$")) // not in char (x) form
                        throw new QException(0, 411, "Wrong definition of char length in " + attrParas[0]);

                    attrLength = attrParas[2].substring(1, attrParas[2].length() - 1); // get length
                    try {
                        length = Integer.parseInt(attrLength); // check the length
                    } catch (NumberFormatException e) {
                        throw new QException(0, 412,
                                "The char length in " + attrParas[0] + " dosen't match a int type or overflow");
                    }
                    if (length < 1 || length > 255)
                        throw new QException(0, 413, "The char length in " + attrParas[0] + " must be in [1,255] ");
                    endIndex = 3; // expected end index
                } else { // unmatched type
                    throw new QException(0, 414, "Error attribute type " + attrType + " in " + attrParas[0]);
                }

                if (attrParas.length == endIndex) { // check unique constraint
                    attrUnique = false;
                } else if (attrParas.length == endIndex + 1 && attrParas[endIndex].equals("unique")) { // unique
                    attrUnique = true;
                } else { // wrong definition
                    throw new QException(0, 415, "Error constraint definition in " + attrParas[0]);
                }

                if (attrType.equals("char")) { // generate attribute
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()),
                            Integer.parseInt(attrLength), attrUnique);
                } else {
                    attribute = new Attribute(attrName, NumType.valueOf(attrType.toUpperCase()), attrUnique);
                }
                attrVec.add(attribute);
            }
        }

        if (primaryName.equals("")) // check whether set the primary key
            throw new QException(0, 416, "Not specified primary key in table " + tableName);

        Table table = new Table(tableName, primaryName, attrVec); // create table
        API.create_table(tableName, table);
        sql_execute_result = "-->Create table " + tableName + " successfully";
        Region.masterThread.master_connector.send("(CREATE)" + tableName);
    }

    private static void parse_drop_table(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 601, "Not specify table name");
        if (tokens.length != 3)
            throw new QException(0, 602, "Extra parameters in drop table");

        String tableName = tokens[2]; // get table name
        API.drop_table(tableName);
        sql_execute_result = "-->Drop table " + tableName + " successfully";
        Region.masterThread.master_connector.send("(DROP)" + tableName);
    }

    private static void parse_create_index(String statement) throws Exception {
        statement = statement.replaceAll("\\s+", " ");
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.trim();

        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 701, "Not specify index name");

        String indexName = tokens[2]; // get index name
        if (tokens.length == 3 || !tokens[3].equals("on"))
            throw new QException(0, 702, "Must add keyword 'on' after index name " + indexName);
        if (tokens.length == 4)
            throw new QException(0, 703, "Not specify table name");

        String tableName = tokens[4]; // get table name
        if (tokens.length == 5)
            throw new QException(0, 704, "Not specify attribute name in table " + tableName);

        String attrName = tokens[5];
        if (!attrName.matches("^\\(.*\\)$")) // not as (xxx) form
            throw new QException(0, 705, "Error in specifiy attribute name " + attrName);

        attrName = attrName.substring(1, attrName.length() - 1); // extract attribute name
        if (tokens.length != 6)
            throw new QException(0, 706, "Extra parameters in create index");
        if (!CatalogManager.is_unique(tableName, attrName))
            throw new QException(1, 707, "Not a unique attribute");

        Index index = new Index(indexName, tableName, attrName);
        API.create_index(index);
        sql_execute_result = "-->Create index " + indexName + " successfully";
    }

    private static void parse_drop_index(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length == 2)
            throw new QException(0, 801, "Not specify index name");
        if (tokens.length != 3)
            throw new QException(0, 802, "Extra parameters in drop index");

        String indexName = tokens[2]; // get table name
        API.drop_index(indexName);
        sql_execute_result = "-->Drop index " + indexName + " successfully";
    }

    private static void parse_select(String statement) throws Exception {
        // select ... from ... where ...
        String attrStr = Utils.substring(statement, "select ", " from");
        String tabStr = Utils.substring(statement, "from ", " where");
        String conStr = Utils.substring(statement, "where ", "");
        Vector<Condition> conditions;
        Vector<String> attrNames;
        long startTime, endTime;
        startTime = System.currentTimeMillis();
        if (attrStr.equals(""))
            throw new QException(0, 250, "Can not find key word 'from' or lack of blank before from!");
        if (attrStr.trim().equals("*")) {
            // select all attributes
            if (tabStr.equals("")) { // select * from [];
                tabStr = Utils.substring(statement, "from ", "");
                Vector<TableRow> ret = API.select(tabStr, new Vector<>(), new Vector<>());
                endTime = System.currentTimeMillis();
                sql_execute_result = Utils.print_rows(ret, tabStr);
            } else { // select * from [] where [];
                String[] conSet = conStr.split(" *and *");
                // get condition vector
                conditions = Utils.create_conditon(conSet);
                Vector<TableRow> ret = API.select(tabStr, new Vector<>(), conditions);
                endTime = System.currentTimeMillis();
                sql_execute_result = Utils.print_rows(ret, tabStr);
            }
        } else {
            attrNames = Utils.convert(attrStr.split(" *, *")); // get attributes list
            if (tabStr.equals("")) { // select [attr] from [];
                tabStr = Utils.substring(statement, "from ", "");
                Vector<TableRow> ret = API.select(tabStr, attrNames, new Vector<>());
                endTime = System.currentTimeMillis();
                sql_execute_result = Utils.print_rows(ret, tabStr);
            } else { // select [attr] from [table] where
                String[] conSet = conStr.split(" *and *");
                // get condition vector
                conditions = Utils.create_conditon(conSet);
                Vector<TableRow> ret = API.select(tabStr, attrNames, conditions);
                endTime = System.currentTimeMillis();
                sql_execute_result = Utils.print_rows(ret, tabStr);
            }
        }
        double usedTime = (endTime - startTime) / 1000.0;
        sql_execute_result += "\nFinished in " + usedTime + " s";
    }

    private static void parse_insert(String statement) throws Exception {
        statement = statement.replaceAll(" *\\( *", " (").replaceAll(" *\\) *", ") ");
        statement = statement.replaceAll(" *, *", ",");
        statement = statement.trim();
        statement = statement.replaceAll("^insert", "").trim(); // skip insert keyword

        int startIndex, endIndex;
        if (statement.equals(""))
            throw new QException(0, 901, "Must add keyword 'into' after insert ");

        endIndex = statement.indexOf(" "); // check into keyword
        if (endIndex == -1)
            throw new QException(0, 902, "Not specify the table name");
        if (!statement.substring(0, endIndex).equals("into"))
            throw new QException(0, 903, "Must add keyword 'into' after insert");

        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); // check table name
        if (endIndex == -1)
            throw new QException(0, 904, "Not specify the insert value");

        String tableName = statement.substring(startIndex, endIndex); // get table name
        startIndex = endIndex + 1;
        endIndex = statement.indexOf(" ", startIndex); // check values keyword
        if (endIndex == -1)
            throw new QException(0, 905, "Syntax error: Not specify the insert value");

        if (!statement.substring(startIndex, endIndex).equals("values"))
            throw new QException(0, 906, "Must add keyword 'values' after table " + tableName);

        startIndex = endIndex + 1;
        if (!statement.substring(startIndex).matches("^\\(.*\\)$")) // check brackets
            throw new QException(0, 907, "Can't not find the insert brackets in table " + tableName);

        String[] valueParas = statement.substring(startIndex + 1).split(","); // get attribute tokens
        TableRow tableRow = new TableRow();

        for (int i = 0; i < valueParas.length; i++) {
            if (i == valueParas.length - 1) // last attribute
                valueParas[i] = valueParas[i].substring(0, valueParas[i].length() - 1);
            if (valueParas[i].equals("")) // empty attribute
                throw new QException(0, 908, "Empty attribute value in insert value");
            if (valueParas[i].matches("^\".*\"$") || valueParas[i].matches("^\'.*\'$")) // extract from '' or " "
                valueParas[i] = valueParas[i].substring(1, valueParas[i].length() - 1);
            tableRow.add_attribute_value(valueParas[i]); // add to table row
        }

        // Check unique attributes
        if (tableRow.get_attribute_size() != CatalogManager.get_attribute_num(tableName))
            throw new QException(1, 909, "Attribute number doesn't match");
        Vector<Attribute> attributes = CatalogManager.get_table(tableName).attributeVector;
        for (int i = 0; i < attributes.size(); i++) {
            Attribute attr = attributes.get(i);
            if (attr.isUnique) {
                Condition cond = new Condition(attr.attributeName, "=", valueParas[i]);
                if (CatalogManager.is_index_key(tableName, attr.attributeName)) {
                    Index idx = CatalogManager
                            .get_index(CatalogManager.get_index_name(tableName, attr.attributeName));
                    if (IndexManager.select(idx, cond).isEmpty())
                        continue;
                } else {
                    Vector<Condition> conditions = new Vector<>();
                    conditions.add(cond);
                    Vector<TableRow> res = RecordManager.select(tableName, conditions); // Supposed to be empty
                    if (res.isEmpty())
                        continue;
                }
                throw new QException(1, 910, "Duplicate unique key: " + attr.attributeName);
            }
        }
        API.insert_row(tableName, tableRow);
        sql_execute_result = "-->Insert successfully";
    }

    private static void parse_delete(String statement) throws Exception {
        // delete from [tabName] where []

        int num;
        String tabStr = Utils.substring(statement, "from ", " where").trim();
        String conStr = Utils.substring(statement, "where ", "").trim();
        Vector<Condition> conditions;
        if (tabStr.equals("")) { // delete from ...
            tabStr = Utils.substring(statement, "from ", "").trim();
            num = API.delete_row(tabStr, new Vector<>());
            sql_execute_result = "Query ok! " + num + " row(s) are deleted";
        } else { // delete from ... where ...
            String[] conSet = conStr.split(" *and *");
            // get condition vector
            conditions = Utils.create_conditon(conSet);
            num = API.delete_row(tabStr, conditions);
            sql_execute_result = "Query ok! " + num + " row(s) are deleted";
        }
    }

    private static void parse_store(String statement) throws Exception {
        String[] tokens = statement.split(" ");
        if (tokens.length != 1)
            throw new QException(0, 1001, "Extra parameters in quit");

        API.store();
        System.out.println("Data stored");
        System.exit(0);
    }
}

class Utils {

    public static final int NONEXIST = -1;
    public static final String[] OPERATOR = { "<>", "<=", ">=", "=", "<", ">" };

    public static String substring(String str, String start, String end) {
        String regex = start + "(.*)" + end;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find())
            return matcher.group(1);
        else
            return "";
    }

    public static <T> Vector<T> convert(T[] array) {
        Vector<T> v = new Vector<>();
        for (int i = 0; i < array.length; i++)
            v.add(array[i]);
        return v;
    }

    public static Vector<Condition> create_conditon(String[] conSet) throws Exception {
        Vector<Condition> c = new Vector<>();
        for (int i = 0; i < conSet.length; i++) {
            int index = contains(conSet[i], OPERATOR);
            if (index == NONEXIST)
                throw new Exception("Syntax error: Invalid conditions " + conSet[i]);
            String attr = substring(conSet[i], "", OPERATOR[index]).trim();
            String value = substring(conSet[i], OPERATOR[index], "").trim().replace("\'", "").replace("\"", "");
            c.add(new Condition(attr, OPERATOR[index], value));
        }
        return c;
    }

    public static boolean check_type(String attr, boolean flag) {
        return true;
    }

    public static int contains(String str, String[] reg) {
        for (int i = 0; i < reg.length; i++) {
            if (str.contains(reg[i]))
                return i;
        }
        return NONEXIST;
    }

    public static int get_max_attr_length(Vector<TableRow> tab, int index) {
        int len = 0;
        for (int i = 0; i < tab.size(); i++) {
            int v = tab.get(i).get_attribute_value(index).length();
            len = v > len ? v : len;
        }
        return len;
    }

    public static String print_rows(Vector<TableRow> tab, String tabName) throws Exception {
        if (tab.size() == 0) {
            return "-->Query ok! 0 rows are selected";
        }
        int attrSize = tab.get(0).get_attribute_size();
        Vector<Integer> v = new Vector<>(attrSize);
        for (int j = 0; j < attrSize; j++) {
            int len = get_max_attr_length(tab, j);
            String attrName = CatalogManager.get_attribute_name(tabName, j);
            if (attrName.length() > len)
                len = attrName.length();
            v.add(len);
        }
        List<Map<String, String>> list = new ArrayList<>();
        for (int i = 0; i < tab.size(); i++) {
            TableRow row = tab.get(i);
            Map<String, String> inputParams = new HashMap<String, String>();
            for (int j = 0; j < attrSize; j++) {
                String attribute_name = "\"" + CatalogManager.get_attribute_name(tabName, j) + "\"";
                String attribute_value = "\"" + row.get_attribute_value(j) + "\"";
                inputParams.put(attribute_name, attribute_value);
            }
            list.add(inputParams);
        }
        return list.toString() + "$-->Query ok! " + tab.size() + " rows are selected";
    }
}