import org.apache.flink.api.common.functions.FlatMapFunction

import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File, Stats}
import util.{CommitGeoParser, CommitParser}

import java.util.Date



/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
      * Setups the streaming environment including loading and parsing of the datasets.
      *
      * DO NOT TOUCH!
      */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    question_eight(commitStream,commitGeoStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
    * Write a Flink application which outputs the sha of commits with at least 20 additions.
    * Output format: sha
    */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input.filter(x => x.stats.isDefined && x.stats.get.additions >= 20).map(_.sha)
  }

  /**
    * Write a Flink application which outputs the names of the files with more than 30 deletions.
    * Output format:  fileName
    */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input.flatMap(_.files).filter(_.deletions > 30).filter(_.filename.isDefined).map(_.filename.get)
  }

  /**
    * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
    * Output format: (fileExtension, #occurrences)
    */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(_.files).filter(_.filename.isDefined).map(_.filename.get)
      .map(name => {
            // Get the index of the last . in the filename
            val dotIndex = name.lastIndexOf(".")
            // Extract the extension if present, otherwise assign "unknown"
            if (dotIndex >= 0 && dotIndex < name.length - 1) {
              name.substring(dotIndex + 1) // Get the file extension
            } else {
              "..." // No extension or invalid file name
            }
      })
      .filter(x =>  x.equals("scala") || x.equals("java"))
      .map(x => (x, 1))
      .keyBy(_._1)
      .sum(1)
  }

  /**
    * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
    * Output format: (extension, status, count)
    */
  def question_four(
      input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input.flatMap(_.files).filter(_.filename.isDefined).filter(_.status.isDefined)
      .map(file => {
        val dotIndex = file.filename.get.lastIndexOf(".")
        if (dotIndex >= 0 && dotIndex < file.filename.get.length - 1) {
          ((file.filename.get.substring(dotIndex + 1), file.status.get), file.changes)
        } else {
          (("...", file.status.get), file.changes)
        }
      })
      .filter(x => x._1._1.equals("js") || x._1._1.equals("py"))
      .keyBy(_._1)
      .sum(1)
      .map(x => (x._1._1, x._1._2, x._2))
  }

  /**
    * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
    * Make use of a non-keyed window.
    * Output format: (date, count)
    */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    val formatter = new SimpleDateFormat("dd-MM-yyyy")
    input//.map(x => (x.commit.committer.date, 1))
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .map(x => (formatter.format(x.commit.committer.date), 1))
      //      .keyBy(_._1)
      //      .timeWindow(Time.days(1))
      .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
      .map(x => (x._1.toString, x._2))
  }



  /**
    * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
    * Compute every 12 hours the amount of small and large commits in the last 48 hours.
    * Output format: (type, count)
    */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .map(x => x.stats.get.total)
      .map(x => {
        if (x <= 20) ("small", 1)
        else ("large", 1)
      })
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
      .sum(1)
  }

  /**
    * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
    *
    * The fields of this case class:
    *
    * repo: name of the repo.
    * date: use the start of the window in format "dd-MM-yyyy".
    * amountOfCommits: the number of commits on that day for that repository.
    * amountOfCommitters: the amount of unique committers contributing to the repository.
    * totalChanges: the sum of total changes in all commits.
    * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
    *
    * Hint: Write your own ProcessWindowFunction.
    * Output format: CommitSummary
    */
  def question_seven(
                      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {

    def getFullRepo(url: String): String = {
      url.substring(29,url.indexOf('/', url.indexOf('/', 29)+1))
    }
    class processCommitSummary extends ProcessWindowFunction[Commit, CommitSummary, String, TimeWindow] {

      def process(key: String, context: Context, elements: Iterable[Commit], out: Collector[CommitSummary])  = {
        val formatter = new SimpleDateFormat("dd-MM-yyyy")
        val date = formatter.format(context.window.getStart)
        val amountOfCommits = elements.size
        val amountOfCommitters = elements.map(x => x.commit.committer.name).toList.distinct.size
        val totalChanges = elements.map(x => x.stats.getOrElse(Stats(0, 0, 0)).total).sum
        val commitCountsPerCommitter = elements.map(x => (x.commit.committer.name, 1)).groupBy(x => x._1).map(x => (x._1, x._2.reduce((y, z) => (y._1, y._2 + z._2))))
          .values
        val maxCommits = commitCountsPerCommitter.map(_._2).max
        val topCommitter = commitCountsPerCommitter.filter(_._2 == maxCommits).map(x=>x._1).toList.sorted.mkString(",")
        out.collect(CommitSummary(key, date, amountOfCommits, amountOfCommitters, totalChanges, topCommitter))
      }
    }

    commitStream
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .keyBy(x => getFullRepo(x.url))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new processCommitSummary())
      .filter(x => x.amountOfCommits > 20 && x.amountOfCommitters <= 2)
  }

  /**
    * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
    * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
    * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
    *
    * Hint: Find the correct join to use!
    * Output format: (continent, amount)
    */
  def question_eight(
      commitStream: DataStream[Commit],
      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {

    val geos = geoStream.assignAscendingTimestamps(_.createdAt.getTime)
    .keyBy(_.sha)

    val SHAChanges = commitStream
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .map(x => (x.sha, x.files.filter(y => y.filename.get.endsWith(".java")).map(y => y.changes).sum)) // (Commit SHA, .java changes) for every commit
      .keyBy(_._1)

    SHAChanges
      .intervalJoin(geos)
      .between(Time.minutes(-60), Time.minutes(30))
      .process(new ProcessJoinFunction[(String, Int), CommitGeo, (String, Int)] {
        override def processElement(left: (String, Int), right: CommitGeo, ctx: ProcessJoinFunction[(String, Int),CommitGeo, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
          out.collect((right.continent, left._2))
        }
      })
      .map(x => (x._1, x._2))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .sum(1)
      .filter(x => x._2 != 0)

//    val geos = geoStream.assignAscendingTimestamps(_.createdAt.getTime)
//
//    val SHAChanges = commitStream
//      .assignAscendingTimestamps(_.commit.committer.date.getTime)
//      .map(x => (x.sha, x.files.filter(y => {
//        val fileName = y.filename.getOrElse("filtered")
//        val dotIndex = fileName.lastIndexOf(".")
//        dotIndex != -1 && fileName.substring(dotIndex).equals(".java")
//      }).map(y => y.changes).sum)) // (Commit SHA, .java changes) for every commit
//
//    geos
//      .join(SHAChanges)
//      .where(x => x.sha)
//      .equalTo(x => x._1)
//      .window(TumblingEventTimeWindows.of(Time.days(7)))
//      .apply((x,y) => (x.continent,y._2))
//      .keyBy(x => x._1)
//      .sum(1)
  }

  /**
    * Find all files that were added and removed within one day. Output as (repository, filename).
    *
    * Hint: Use the Complex Event Processing library (CEP).
    * Output format: (repository, filename)
    */
  def question_nine(
      inputStream: DataStream[Commit]): DataStream[(String, String)] = ???

}
