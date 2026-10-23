# Hands-on L9: Report

**Name:** Soumyanil Ain
**Student ID:** 801488534
**Email:** sain@charlotte.edu

---

## Seed and commands

Seed used for `datagen.py`: **801488534**

I worked on Windows in PowerShell with Docker Desktop. The commands I ran, in order:

```bash
# my copy of the template, cloned to the D: drive
cd D:\
git clone https://github.com/soumyanilain/ITCS6190-H9-SoumyanilAin.git
cd D:\ITCS6190-H9-SoumyanilAin

# generate the data
python datagen.py 801488534

# start the cluster and check http://localhost:8080 for two ALIVE workers
docker compose up -d

# copy the script in and run it (repeated after each change to main.py)
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output

# stop the cluster
docker compose down
```

Deviations from the README: on Windows the command is `python`, not `python3`, and I put
the `spark-submit` call on one line because PowerShell doesn't use `\` for line continuation.

---

## Results

### Task 1: hashtag trends (DataFrame API)

```
+---------+-----+
|hashtag  |count|
+---------+-----+
|#social  |1667 |
|#love    |1570 |
|#ux      |1563 |
|#design  |1060 |
|#darkmode|1032 |
|#fail    |664  |
|#update  |621  |
|#ai      |602  |
|#cleanui |474  |
|#tech    |257  |
+---------+-----+
```

The top three hashtags are close together and make up nearly half of all 9,981 hashtag
uses. After that there is a clear drop to `#design` and `#darkmode` (about 1,000 each), and
then a long tail. `#mood` (239) and `#bug` (232) just miss the top ten.

How many distinct spellings did your most frequent hashtag have before `lower` and `trim`?
**Two:** `#social` (1,399 times) and `#SOCIAL` (268 times). The generator writes a tag in
lower case, upper case, or its original form, and the original form of `#social` is already
all lower case, so it can only ever have two spellings. A mixed-case tag like `#cleanUI`
can have three. Across the whole file there were 25 raw spellings, and normalizing
collapsed them into 12 hashtags.

### Task 2: engagement by age group (Spark SQL)

```
+---------+-----+---------+------------+
|age_group|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Senior   |1317 |52.9     |12.78       |
|Adult    |1083 |50.57    |12.64       |
|Teen     |2600 |48.73    |11.95       |
+---------+-----+---------+------------+
```

Teens write more than half of all posts (2,600 of 5,000). That's because 21 of the 50
users are teens, and four of the five heaviest posters are teens. Seniors get the most
likes per post, but the gap is small (about 4 likes) and it has nothing to do with age.
Verified users get about 1.6 times the engagement, and 32.4% of Senior posts come from
verified accounts, compared with 20.0% for Adults and 18.0% for Teens. The ranking just
follows the share of verified posts in each group.

### Task 3: sentiment versus engagement (Spark SQL)

```
+---------+-----+---------+------------+
|sentiment|posts|avg_likes|avg_retweets|
+---------+-----+---------+------------+
|Negative |1074 |22.66    |6.19        |
|Neutral  |2203 |46.7     |11.59       |
|Positive |1723 |71.92    |17.08       |
+---------+-----+---------+------------+
```

Does sentiment pay? Yes, and by a lot. A positive post averages 71.92 likes against 22.66
for a negative one, about 3.2 times as many, and 17.08 retweets against 6.19, about 2.8
times as many. Neutral posts land almost exactly halfway on both. This is the biggest
effect in the dataset, far bigger than the age group differences in task 2.

### Task 4: top verified users (DataFrame API)

```
+---------------+-----------+--------------+-----+
|username       |total_likes|total_retweets|reach|
+---------------+-----------+--------------+-----+
|@calm_mind1    |23488      |5780          |29268|
|@night_coder2  |21606      |5338          |26944|
|@critic16      |6895       |1804          |8699 |
|@calm_mind10   |4396       |1115          |5511 |
|@coffee_first13|4169       |1150          |5319 |
+---------------+-----------+--------------+-----+
```

The top two are in a league of their own: both have more than three times the reach of
third place. That gap comes from volume, not quality. `@calm_mind1` and `@night_coder2`
posted 323 and 316 times, while the other three posted 103, 60 and 60 times. Per post, all
five are about the same (roughly 67 to 73 likes). That's well above the 44.54 likes a
non-verified user's post gets, which is the verified boost again.

---

## Two languages, one plan

The two `explain()` outputs of task 2:

SQL version:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#53 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#53 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=697]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=694]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=689]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

DataFrame version:

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [avg_likes#133 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(avg_likes#133 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=747]
      +- HashAggregate(keys=[age_group#10], functions=[count(1), avg(likes#4), avg(retweets#5)])
         +- Exchange hashpartitioning(age_group#10, 200), ENSURE_REQUIREMENTS, [plan_id=744]
            +- HashAggregate(keys=[age_group#10], functions=[partial_count(1), partial_avg(likes#4), partial_avg(retweets#5)])
               +- Project [likes#4, retweets#5, age_group#10]
                  +- BroadcastHashJoin [user_id#1], [user_id#8], Inner, BuildRight, false, false
                     :- Filter isnotnull(user_id#1)
                     :  +- FileScan csv [user_id#1,likes#4,retweets#5] Batched: false, DataFilters: [isnotnull(user_id#1)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/posts.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,likes:int,retweets:int>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=739]
                        +- Filter isnotnull(user_id#8)
                           +- FileScan csv [user_id#8,age_group#10] Batched: false, DataFilters: [isnotnull(user_id#8)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/users.csv], PartitionFilters: [], PushedFilters: [IsNotNull(user_id)], ReadSchema: struct<user_id:string,age_group:string>
```

**Are they the same?** Yes, operator for operator. The only differences are the numbers
after `#` and in `plan_id`. Those are internal IDs that Spark hands out as it builds each
query, and they don't change what runs. The input columns even keep the same IDs
(`likes#4`, `age_group#10`) in both plans, because both queries read the same `posts` and
`users` DataFrames.

Reading the plans from the bottom up:

- **Scans.** Two `FileScan csv` operators, each reading only the columns the query needs.
  From `posts` that's `user_id`, `likes` and `retweets`, so `content` and `hashtags` are
  never read. Spark also adds an `isnotnull(user_id)` filter on both sides, since an inner
  join can't match a null key anyway.
- **The join** is `BroadcastHashJoin ... Inner, BuildRight`. `users` is only 50 rows (about
  2 KB), far under Spark's broadcast threshold (10 MB by default). So Spark sends a full
  copy of `users` to every executor (the `BroadcastExchange`), builds a hash table from it
  on the right side, and streams `posts` through it where it already sits. `posts` is
  never shuffled for the join.
- **The aggregation happens in two steps.** First, a `HashAggregate` with `partial_count`
  and `partial_avg` runs on each partition and produces at most three rows per partition,
  one per age group. Then a second `HashAggregate` combines those partial rows into the
  final count and averages.

**Where the `Exchange` operators are, and why they're needed:**

- `Exchange hashpartitioning(age_group, 200)` sits between the two aggregation steps. The
  final aggregate needs every partial result for the same age group on the same partition,
  so rows are shuffled by a hash of `age_group`. Because the partial aggregate runs first,
  only a handful of small rows cross the network instead of 5,000 posts.
- `Exchange rangepartitioning(avg_likes DESC, 200)` sits just under the `Sort`. `ORDER BY`
  needs a global order, not just order within each partition. Range partitioning puts
  rows into partitions by value range, so sorting each partition gives a correct overall
  order.
- The `BroadcastExchange` is the third data movement, but it's a broadcast of a small
  table, not a shuffle.

`AdaptiveSparkPlan isFinalPlan=false` means adaptive query execution is on. At runtime,
Spark can shrink those 200 shuffle partitions down to a few, since there are only three
groups.

**What this tells me about choosing between SQL and the DataFrame API:** both go through
the same Catalyst optimizer and end up as the same physical plan, so there is no
performance reason to prefer one over the other. The choice comes down to what reads
better for the problem and the people working on it. SQL is clearer for a fixed join and
aggregation like task 2 or a `CASE` classification like task 3. The DataFrame API is
easier when the query is built up step by step in code, like task 1's split, explode and
normalize chain, or task 4's filter followed by a computed column. You can also mix the
two freely in one program, as this assignment does.

---

## Views

`createOrReplaceTempView("posts")` gave the `posts` DataFrame a name in the SparkSession's
catalog so that SQL strings can refer to it. If a view with that name already existed, it
was replaced.

It did **not** read or copy any data. A DataFrame is a lazy plan ("read this CSV with this
schema"), and the view is just that plan stored under a name. When `spark.sql` mentions
`posts`, Spark swaps in the plan, which still points at `posts.csv`. That's why the SQL and
DataFrame versions of task 2 show the exact same `FileScan csv` at the bottom. Nothing is
cached either, so each query that uses the view reads the file again unless you call
`cache()`.

A temp view only lives as long as the SparkSession that created it. When the program
calls `spark.stop()` and ends, the views are gone, and so is everything else in that
session's catalog. Another application, or even another session in the same application,
can't see them. A global temp view (`createOrReplaceGlobalTempView`, queried as
`global_temp.posts`) is shared across sessions, but it still dies with the application.

To keep a table for the next session, the data has to be written somewhere that outlives
the program. One way is `posts.write.saveAsTable("posts")`, which writes the files to the
Spark warehouse and registers the table in the metastore. The metastore needs to be
persistent (for example Hive support enabled) for the table name to survive. Another way
is to write the data out as Parquet and create a view over that path at the start of the
next session. That way you also skip parsing the CSV again.

---

## Problems and fixes

The Spark job itself ran without errors.

The one problem was with git. The README says the `_SUCCESS` and `.crc` files and the
generated input CSVs are ignored by `.gitignore`, but the template doesn't actually include
a `.gitignore`. After the run, `shared-folder/output/task1` to `task4` each contained
`_SUCCESS`, `._SUCCESS.crc` and a `.part-...csv.crc` file next to the real result, and
`shared-folder/input` had `posts.csv` and `users.csv`. A plain `git add .` would have
committed all of them. I fixed this by adding a `.gitignore` that excludes the input CSVs,
`_SUCCESS` and `*.crc`, so only `main.py`, `REPORT.md` and the four `part-*.csv` results get
committed.
