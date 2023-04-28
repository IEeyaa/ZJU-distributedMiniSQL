# ReadMe

## 使用方法

- 运行Test开启客户端
- 运行server、region开启模拟master和region端
  - 模拟server只支持search查询表，且返回ip写死在代码中，需要手动更改



## 说明



## 待完善

- √ getTableName正式使用 
  - √；问题已解决
  -  **√ 多个空格不友好 问题**
- **√ 基本完成  select返回值展示**
- 多个client，多个region未测试
- √ socket 连接失败抛出异常处理
- √ 旧cache数据问题：
  - client的cache，有种情况我之前没考虑到：就是cache里的ip数据旧了，但还是会优先连cache里的ip数据。如果cache里的ip是联不通的，client可以自己处理异常。如果cache里的ip连得通，但region已经不存对应的表了，你那边应该会有一个表不存在error，我希望这个error能格式独立出来，因为不能像其他错误一样直接在客户端print，而是要重新问master
  - 把client处理SQL语句的部分独立出来process函数，这样如果region连不到，可以直接删掉对应的cache，调用client的process函数【考虑一下会不会互相调用无限线程

## 更新日志

-----2022.5.17----

- 上传工程文件
- 删除了之前的src

--------2022.05.08---------

- 仅上传了client的src文件
- 运行Test开启客户端
- 运行server、region开启模拟master和region端
- 可能出现GBK中文错误
  - 如果用的是VSCode，重新save with encoding GBK
- getTableName函数未使用，目前指令提取的tablename全为”table“







```java
public static void print_rows(Vector<TableRow> tab, String tabName, BufferedWriter writer) throws Exception {
       
        int attrSize = tab.get(0).get_attribute_size();
        int cnt = 0;
        System.out.println("mark");
        Vector<Integer> v = new Vector<>(attrSize);
        for (int j = 0; j < attrSize; j++) {
            int len = get_max_attr_length(tab, j);
            String attrName = CatalogManager.get_attribute_name(tabName, j);
            if (attrName.length() > len) len = attrName.length();
            v.add(len);
            String format = "|%-" + len + "s";
            System.out.printf(format, attrName);
            cnt = cnt + len + 1;
        }
        System.out.println("mark");
        cnt++;
        System.out.println("|");
        for (int i = 0; i < cnt; i++) System.out.print("-");
        System.out.println();
        System.out.println("mark");
        List<Map<String, String>> list = new ArrayList<>();
        for (int i = 0; i < tab.size(); i++) {
            TableRow row = tab.get(i);
            Map<String, String> inputParams = new HashMap<String, String>();
            for (int j = 0; j < attrSize; j++) {
                String format = "|%-" + v.get(j) + "s";
                System.out.printf(format, row.get_attribute_value(j));
                String attribute_name = "\"" + CatalogManager.get_attribute_name(tabName, j) + "\"";
                String attribute_value = "\"" + row.get_attribute_value(j) + "\"";
                inputParams.put(attribute_name, attribute_value);
            }
            list.add(inputParams);
            System.out.println("|");
        }
        writer.write(list.toString());
        writer.newLine();
        writer.flush();
        System.out.println("-->Query ok! " + tab.size() + " rows are selected");
    }
```

