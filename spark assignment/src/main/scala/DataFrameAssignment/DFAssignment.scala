package DataFrameAssignment

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, datediff, dayofweek, dayofyear, explode, from_unixtime, lag, lit, to_timestamp, unix_timestamp, when, year}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, datediff, dayofweek, dayofyear, from_unixtime, lag, lit, to_timestamp, unix_timestamp, when, year}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.Parent

import java.sql.{Date, Struct, Timestamp}
import scala.collection.mutable

/**
  * Please read the comments carefully, as they describe the expected result and may contain hints in how
  * to tackle the exercises. Note that the data that is given in the examples in the comments does
  * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
  */
object DFAssignment {

  /**
    * In this exercise we want to know all the commit SHA's from a list of committers. We require these to be
    * ordered according to their timestamps in the following format:
    *
    * | committer      | sha                                      | timestamp            |
    * |----------------|------------------------------------------|----------------------|
    * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
    * | ...            | ...                                      | ...                  |
    *
    * Hint: Try to work out the individual stages of the exercises. This makes it easier to track bugs, and figure out
    * how Spark DataFrames and their operations work. You can also use the `printSchema()` function and `show()`
    * function to take a look at the structure and contents of the DataFrames.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @param authors Sequence of Strings representing the authors from which we want to know their respective commit
    *                SHA's.
    * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
    */
  def assignment_12(commits: DataFrame, authors: Seq[String]): DataFrame = {
    commits.select("commit.committer.name", "sha", "commit.committer.date").toDF("committer", "sha", "timestamp").sort(col("timestamp"))
      .filter(x => authors.contains(x.getString(0)))

//    println(commits.schema)
//    commits.select("commit.committer.name", "sha", "commit.committer.date")
//      .filter(
//        commits("commit.committer.name")
//          .isin(authors: _*))
//      .sort("commit.committer.date")
//      .withColumnRenamed("commit.committer.name", "committer")
//      .withColumnRenamed("date", "timestamp")
  }

  /**
    * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
    * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
    *
    * Expected DataFrame example:
    *
    * | repository | week             | year | count   |
    * |------------|------------------|------|---------|
    * | Maven      | 41               | 2019 | 21      |
    * | .....      | ..               | .... | ..      |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
   *                `println(commits.schema)`.
    * @return DataFrame containing 4 columns: repository name, week number, year and the number of commits for that
    *         week.
    */
  def assignment_13(commits: DataFrame): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder
      .config("spark.driver.host", "localhost")
      .appName("Spark-Assignment")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    def getRepo(url: String): String = {
      url.substring(url.indexOf('/', 29)+1,url.indexOf('/', url.indexOf('/', 29)+1))
    }
    commits.select("url", "commit.committer.date")
      .map(x => (getRepo(x.getString(0))
        ,Date.valueOf(x.getString(1).substring(0,10)).toLocalDate.getDayOfYear/7+1
        ,Date.valueOf(x.getString(1).substring(0,10)).toLocalDate.getYear))
      .groupBy("_1", "_2", "_3").count().toDF("repository", "week", "year", "count")

    //    // Extract the repository name from the URL (assuming the repository name follows the structure like "owner/repo")
    //    val extractRepoName = commits
    //      .withColumn("repository", substring_index(substring_index(commits("url"), "/", 5), "/", -1))
    //
    //    // Extract the year and week from the commit date (assuming the commit date is in a standard timestamp format)
    //    val extractYearAndWeek = extractRepoName
    //      .withColumn("commit_date", to_date(extractRepoName("commit.committer.date")))
    //      .withColumn("year", year(extractRepoName("commit.committer.date")))
    //      .withColumn("week", weekofyear(extractRepoName("commit.committer.date")))
    //
    //    // Group by repository, year, and week, and count the number of commits per group
    //    val weeklyCommitCounts = extractYearAndWeek
    //      .groupBy("repository", "week", "year")
    //      .agg(functions.count("*").as("count"))
    //
    //    // Return the resulting DataFrame with the desired columns
    //    weeklyCommitCounts.filter(weeklyCommitCounts("repository") === "DrTests").show(10)
    //    weeklyCommitCounts
  }

  /**
    * A developer is interested in the age of commits in seconds. Although this is something that can always be
    * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore, we
    * require you to **append** the input DataFrame with an `age` column of each commit in *seconds*.
    *
    * Hint: Look into SQL functions for Spark SQL.
    *
    * Expected DataFrame (column) example:
    *
    * | age    |
    * |--------|
    * | 1231   |
    * | 20     |
    * | ...    |
    *
    * @param commits Commit DataFrame, created from the data_raw.json file.
    * @return the input DataFrame with the appended `age` column.
    */
  def assignment_14(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder
      .config("spark.driver.host", "localhost")
      .appName("Spark-Assignment")
      .master("local[*]")
      .getOrCreate()

    commits.withColumn("date", to_timestamp(col("commit.committer.date")))
      .withColumn("timeNow",lit(snapShotTimestamp))
      .createOrReplaceTempView("commits")

    spark.sqlContext.sql("SELECT *, (unix_timestamp(timeNow)-unix_timestamp(date)) AS age FROM commits").drop("date").drop("timeNow")
  }

  /**
    * To perform the analysis on commit behavior, the intermediate time of commits is needed. We require that the DataFrame
    * that is given as input is appended with an extra column. his column should express the number of days there are between
    * the current commit and the previous commit of the user, independent of the branch or repository.
    * If no commit exists before a commit, the time difference in days should be zero.
    * **Make sure to return the commits in chronological order**.
    *
    * Hint: Look into Spark SQL's Window to have more expressive power in custom aggregations.
    *
    * Expected DataFrame example:
    *
    * | $oid                     	| name   	| date                     	| time_diff 	|
    * |--------------------------	|--------	|--------------------------	|-----------	|
    * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
    * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
    * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
    * | ...                      	| ...    	| ...                      	| ...       	|
    *
    * @param commits    Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                   `println(commits.schema)`.
    * @param authorName Name of the author for which the result must be generated.
    * @return DataFrame with an appended column expressing the number of days since last commit, for the given user.
    */
  def assignment_15(commits: DataFrame, authorName: String): DataFrame = {

//    val spark: SparkSession = SparkSession
//      .builder
//      .config("spark.driver.host", "localhost")
//      .appName("Spark-Assignment")
//      .master("local[*]")
//      .getOrCreate()
//    import spark.implicits._

    val windowSpec = Window.orderBy("date")


    val dfWithDates = commits.filter(commits("commit.committer").getField("name") === authorName)
      .withColumn("date", to_timestamp(col("commit.committer.date")))
      .select("_id", "commit.committer.name", "date")
      .toDF("$oid", "name", "date")
      .sort("date")
      .withColumn("prevDate", lag("date", 1).over(windowSpec))

    val withDiff = dfWithDates.withColumn("time_diff", datediff(dfWithDates("date"), dfWithDates("prevDate")))

    withDiff.withColumn("time_diff",
        when(col("prevDate").isNull, 0)
          .otherwise(col("time_diff")))
      .select("$oid", "name", "date", "time_diff")
  }

  /**
    * To get a bit of insight into the spark SQL and its aggregation functions, you will have to implement a function
    * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits'
    * dates. Sunday would be 1, Monday 2, etc.
    *
    * Expected DataFrame example:
    *
    * | day | commits_per_day|
    * |-----|----------------|
    * | 1   | 32             |
    * | ... | ...            |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing a `day` column and a `commits_per_day` column representing the total number of
    *         commits that have been made on that week day.
    */
  def assignment_16(commits: DataFrame): DataFrame = {
    commits.select("commit.committer.date")
      .withColumn("day", dayofweek(col("date")))
      .groupBy("day").count()
  }

  /**
    * Commits can be uploaded on different days. We want to get insight into the difference in commit time of the author and
    * the committer. Append to the given DataFrame a column expressing the difference in *the number of seconds* between
    * the two events in the commit data.
    *
    * Expected DataFrame (column) example:
    *
    * | commit_time_diff |
    * |------------------|
    * | 1022             |
    * | 0                |
    * | ...              |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return the original DataFrame with an appended column `commit_time_diff`, containing the time difference
    *         (in number of seconds) between authorizing and committing.
    */
  def assignment_17(commits: DataFrame): DataFrame = {
    commits.withColumn("commitTime", unix_timestamp(to_timestamp(col("commit.committer.date"))))
      .withColumn("authorTime", unix_timestamp(to_timestamp(col("commit.author.date"))))
//      .withColumn("commit_time_diff",
//        when(col("authorTime").isNull, 0)
//          .otherwise(unix_timestamp(col("commitTime") - unix_timestamp(col("authorTime")))))
      .withColumn("commit_time_diff", col("commitTime") - col("authorTime"))
      .drop("authorTime", "commitTime")
//      .show(10)
//    commits
  }

  /**
    * Using DataFrames, find all the commit SHA's from which a branch has been created, including the number of
    * branches that have been made. Only take the SHA's into account if they are also contained in the DataFrame.
    *
    * Note that the returned DataFrame should not contain any commit SHA's from which no new branches were made, and it should
    * not contain a SHA which is not contained in the given DataFrame.
    *
    * Expected DataFrame example:
    *
    * | sha                                      | times_parent |
    * |------------------------------------------|--------------|
    * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
    * | ...                                      | ...          |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the commit SHAs from which at least one new branch has been created, and the actual
    *         number of created branches
    */
  def assignment_18(commits: DataFrame): DataFrame = {
    val spark = SparkSession.builder()
      .appName("FlatMap Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val shaValues = commits.select("sha").distinct().collect().map { row: Row =>
      row.getAs[String]("sha") // Extracting the "sha" as a String
    }

    commits.select("sha", "parents")
      //.filter(!col("parents").isNull)
      .withColumn("parent_sha", explode(col("parents")))
      .withColumn("parent", col("parent_sha").getField("sha"))
      .select("parent", "sha")
      .filter(col("parent").isInCollection(shaValues))
      .filter(row => !row.getString(0).equals(row.getString(1)))
      .withColumnRenamed("sha", "child")
      .withColumnRenamed("parent", "sha")
      .groupBy(col("sha")).count()
      .withColumnRenamed("count", "times_parent")
      .filter(col("times_parent") > 1)
      //.sort("times_parent")
      //.show(10)

//    // Step 1: Extract SHA values and parent SHA values
//    // Explode the "parents" column (which contains lists of parent SHAs)
//    val explodedCommits = commits
//      .select(col("sha"), explode(col("parents")).alias("parent_sha"))
//
//    // Step 2: Filter parent SHAs to include only SHAs that are also present in the DataFrame as commit SHAs
//    val shaValues = commits.select("sha").distinct()
//
//    // Step 3: Group by the parent SHA and count how many times it appears (this indicates the number of branches created from it)
//    val branchesCount = explodedCommits
//      .join(shaValues.withColumnRenamed("sha", "sha_value"), explodedCommits("parent_sha") === shaValues("sha"))
//      .groupBy("parent_sha").count()
//      //.agg(count("*").alias("times_parent"))
//
//    // Step 4: Return the result as a DataFrame with the correct column names
//    branchesCount
//      .select(col("parent_sha").alias("sha"), col("times_parent")).show(10)

    //commits
  }

  /**
    * In the given commit DataFrame, find all commits from which a fork has been created. We are interested in the names
    * of the (parent) repository and the subsequent (child) fork (including the name of the repository owner for both),
    * the SHA of the commit from which the fork has been created (parent_sha) as well as the SHA of the first commit that
    * occurs in the forked branch (child_sha).
    *
    * Expected DataFrame example:
    *
    * | repo_name            | child_repo_name     | parent_sha           | child_sha            |
    * |----------------------|---------------------|----------------------|----------------------|
    * | ElucidataInc/ElMaven | saifulbkhan/ElMaven | 37d38cb21ab342b17... | 6a3dbead35c10add6... |
    * | hyho942/hecoco       | Sub2n/hecoco        | ebd077a028bd2169d... | b47db8a9df414e28b... |
    * | ...                  | ...                 | ...                  | ...                  |
    *
    * Note that this example is based on _real_ data, so you can verify the functionality of your solution, which might
    * help during the debugging of your solution.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the parent and child repository names, the SHA of the commit from which a new fork
    *         has been created and the SHA of the first commit in the fork repository
    */
  def assignment_19(commits: DataFrame): DataFrame = {
    val spark: SparkSession = SparkSession
      .builder
      .config("spark.driver.host", "localhost")
      .appName("Spark-Assignment")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    def getFullRepo(url: String): String = {
      url.substring(29,url.indexOf('/', url.indexOf('/', 29)+1))
    }
    val data = commits.select("url","commit.committer.date", "sha", "parents.sha")
      .as[(String, String, String, Array[String])].map(x => x._4.map(y => (x._1,x._2,x._3,y)))
      .flatMap(x => x)
    val parents = data.map(x => (getFullRepo(x._1),x._3)).toDF("repo_name", "sha")
    val children = data.map(x => (getFullRepo(x._1), x._3,x._4)).toDF("repo_name", "sha", "parent_sha")
    parents.createOrReplaceTempView("parents")
    children.createOrReplaceTempView("children")
    spark.sql("SELECT parents.repo_name AS repo_name, children.repo_name AS child_repo_name," +
      "children.parent_sha AS parent_sha, children.sha AS child_sha" +
      " FROM children INNER JOIN parents ON children.parent_sha=parents.sha").filter(x => !x.getString(0).equals(x.getString(1))).distinct()
  }
}
