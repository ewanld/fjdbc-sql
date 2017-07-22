# fjdbc-sql
A SQL statement generator for Java.

## Quickstart

### Set up
```java
Connection cnx = DriverManager.getConnection("/jdbc/url");
SingleConnectionProvider provider = new SingleConnectionProvider(cnx);
OracleSql dsl = new OracleSql(provider);
```

### Select
```java
SqlSelectBuilder builder = dsl.select("ename").from("emp").where("empno").eq().value(1);
SingleRowExtractor<Integer> extractor = rs -> rs.getString("ename");
builder.toQuery(extractor).toList();  // returns ["KING"]
```

## More examples

### Delete
```java
dsl.deleteFrom("emp").where("job").notEq().value("SALESMAN");
````
Generates the following statement:
```SQL
delete from emp
where
    job <> ?  /* SALESMAN */
```

### Update
```java
dsl.update("emp").set("salary").raw("salary * 2");
```
Generates the following statement:
```SQL
update emp set
    salary = salary * 2
```
### Insert
```java
SqlInsertBuilder builder = dsl.insertInto("emp");
builder.values().set("ename").value("KING");
builder.values().set("job").value("PRESIDENT");
```

```SQL
insert into emp(ename, job)
values (?  /* KING */, ?  /* PRESIDENT */)
```
