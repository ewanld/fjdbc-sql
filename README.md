# fjdbc-sql: a safe SQL generator for Java

fjdbc-sql makes it easy to build complex SELECT, UPDATE, INSERT or DELETE statements without having to concatenate SQL string fragments.

It handles regular and prepared statements, as well as batch statements.
This project is built on top of FJDBC, a functional wrapper for JDBC.

Latest version: 0.1.0.

## Quickstart

### Set up
```java
final Connection cnx = DriverManager.getConnection("/jdbc/url");
final SingleConnectionProvider provider = new SingleConnectionProvider(cnx);
final SqlBuilder sql = new SqlBuilder(provider);

```

### Select data from the database
```java
final SqlSelectBuilder builder = sql.select("ename").from("emp").where("empno").eq().value(1);
final SingleRowExtractor<String> extractor = rs -> rs.getString("ename");
builder.toQuery(extractor).toList(); // returns ["KING"]
```

## DELETE examples
```java
sql.deleteFrom("emp")
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
sql.update("emp").set("salary").raw("salary * 2");
```
Generates the following statement:
```SQL
update emp set
    salary = salary * 2
```

## INSERT examples
### Simple INSERT
```java
final SqlInsertBuilder builder = sql.insertInto("emp");
builder.set("ename").value("KING");
builder.set("job").value("PRESIDENT");
builder.set("hiredate").value(new java.sql.Date(new Date().getTime()));
```
Generates the following statement:
```SQL
insert into emp(ename, job, hiredate)
values (?  /* KING */, ?  /* PRESIDENT */, ?  /* 2017-07-22 */)
```

### INSERT with subquery
```java
final SqlInsertBuilder builder = sql.insertInto("emp2");
builder.subquery("ename", "job").select("ename", "job").from("emp");
```
Generates the following statement:
```SQL
insert into emp2 (ename, job)
select ename, job
from emp
```

## SELECT examples
### SELECT with aggregate function
```java
sql.select("job", "count(*)")
.from("emp")
.groupBy("job");
```
Generates the following statement:
```SQL
select job, count(*)
from emp
group by job
```

### Compound statement: UNION
```java
sql.union(
    sql.select("job").from("emp"),
    sql.select("job").from("emp2")
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

### SELECT with AND/OR operators
```java
final SqlSelectBuilder builder = sql.select("*");
builder.from("emp");
builder.where(sql.or(
    sql.condition("job").eq().value("SALESMAN"),
    sql.condition("name").eq().value("KING")
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

### SELECT with JOIN clause
```Java
sql.select("e.ename", "d.deptname")
.from("emp e")
.innerJoin("dept d on e.deptno = d.deptno");
```
Generates the following statement:
```SQL
select e.ename, d.deptname
from emp e
inner join dept d on e.deptno = d.deptno
```

### Select with subquery
```java
sql.select("*")
.from("emp")
.where("deptno").eq().subquery(
    sql.select("deptno")
    .from("dept")
    .where("deptname").eq().value("SALES")
);
```
Generates the following statement:
```SQL
select *
from emp
where
    deptno = (
        select deptno
        from dept
        where
            deptname = ?  /* SALES */
    )
```

### Select with IN clause
```Java
final Collection<String> names = Arrays.asList("KING", "ALLEN");
sql.select("*")
.from("emp")
.where("ename").in_String(names);
```
Generates the following statement:
```SQL
select *
from emp
where
    ename in (?, ?)
```

## Batch statement examples
### Batch statement with input data coming from a Collection
This is the same example as previously, except the data come from a Collection instead of a Stream.
```java
final List<String> values = Arrays.asList("SALES", "ACCOUNTING");
final List<SqlInsertBuilder> inserts = values.stream().map(ReadmeExamples::createInsertStatement)
		.collect(Collectors.toList());
sql.batchStatement(inserts).executeAndCommit();
```

### Batch statement with input data coming from a Stream
In this example, a Stream of strings is converted to a batch sequence of INSERT statements (using the addBatch method in JDBC).
At no point in time the full data is held in memory.
```java
final Stream<SqlInsertBuilder> stream = Files.lines(Paths.get("c:/my/file.txt"))
		.map(ReadmeExamples::createInsertStatement);
// the empty string is used to generate the SQL, but anything else would
// have been fine since we are dealing with a prepared statement.
final String sqlString = createInsertStatement("").getSql();
sql.batchStatement(sqlString, stream).toStatement().executeAndCommit();
```

```java
private static SqlInsertBuilder createInsertStatement(String deptname) {
	final SqlInsertBuilder builder = sql.insertInto("dept");
	builder.values().set("deptname").value(deptname);
	return builder;
}
```
