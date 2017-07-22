# fjdbc-sql
A SQL statement generator for Java.

## Quickstart

### Set up
```java
Connection cnx = DriverManager.getConnection("/jdbc/url");
SingleConnectionProvider provider = new SingleConnectionProvider(cnx);
OracleSql dsl = new OracleSql(provider);
```

### Select data from the database
```java
SqlSelectBuilder builder = dsl.select("ename").from("emp").where("empno").eq().value(1);
SingleRowExtractor<Integer> extractor = rs -> rs.getString("ename");
builder.toQuery(extractor).toList();  // returns ["KING"]
```

## DELETE examples
```java
dsl.deleteFrom("emp")
   .where("job").notEq().value("SALESMAN");
````
Generates the following statement:
```SQL
delete from emp
where
    job <> ?  /* SALESMAN */
```


## UPDATE examples
```java
dsl.update("emp").set("salary").raw("salary * 2");
```
Generates the following statement:
```SQL
update emp set
    salary = salary * 2
```

## INSERT examples
### Simple INSERT
```java
SqlInsertBuilder builder = dsl.insertInto("emp");
builder.values().set("ename").value("KING");
builder.values().set("job").value("PRESIDENT");
builder.values().set("hiredate").value(new java.sql.Date(new Date().getTime()));
```
Generates the following statement:
```SQL
insert into emp(ename, job, hiredate)
values (?  /* KING */, ?  /* PRESIDENT */, ?  /* 2017-07-22 */)
```

### INSERT with subquery
```java
SqlInsertBuilder builder = dsl.insertInto("emp2");
builder.subquery("ename", "job").select("ename", "job").from("emp");
```
Generates the following statement:
```SQL
insert into emp2 (ename, job)
select ename, job
from emp
```

## SELECT examples
### Aggregate function
```java
dsl.select("job", "count(*)")
   .from("emp")
   .groupBy("job");
```
Generates the following statement:
```SQL
select job, count(*)
from emp
group by job
```

### UNION
```java
dsl.union(
    dsl.select("job").from("emp"),
    dsl.select("job").from("emp2")
);
```
Generates the following statement:
```SQL
select job
from emp
union
select job
from emp2
```

### AND/OR conditions
```java
final SqlSelectBuilder builder = dsl.select("*");
builder.from("emp");
builder.where(dsl.or(
    dsl.condition("job").eq().value("SALESMAN"),
    dsl.condition("name").eq().value("KING")
));
// Multiple WHERE clauses are joined with AND
builder.where("hiredate").lt().raw("SYSDATE - 1");
```
Generates the following statement:
```SQL
select *
from emp
where
    (job = ?  /* SALESMAN */ or name = ?  /* KING */)
    and hiredate < SYSDATE - 1
```

### JOIN clause
```Java
dsl.select("e.ename", "d.deptname")
   .from("emp e")
   .innerJoin("dept d on e.deptno = d.deptno");
```
Generates the following statement:
```SQL
select e.ename, d.deptname
from emp e
inner join dept d on e.deptno = d.deptno
```
