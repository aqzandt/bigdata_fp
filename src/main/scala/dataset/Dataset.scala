package dataset

import dataset.util.Commit.Commit

import java.text.SimpleDateFormat
import java.util.SimpleTimeZone
import scala.math.Ordering.Implicits._

/**
 * Use your knowledge of functional programming to complete the following functions.
 * You are recommended to use library functions when possible.
 *
 * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
 * When asked for dates, use the `commit.commit.committer.date` field.
 *
 * This part is worth 40 points.
 */
object Dataset {


  /** Q23 (4p)
   * For the commits that are accompanied with stats data, compute the average of their additions.
   * You can assume a positive amount of usable commits is present in the data.
   *
   * @param input the list of commits to process.
   * @return the average amount of additions in the commits that have stats data.
   */
  def avgAdditions(input: List[Commit]): Int =
    {
      val additions = input.flatMap(Commit => Commit.stats).map(stats => stats.additions)
      additions.sum/additions.size

/*      def getSum(commits: List[Commit]): Int = {
        commits match {
          case Nil => 0
          case i :: tail =>
            i.stats match {
              case None => getSum(tail)
              case x => x.get.additions + getSum(tail)
            }
        }
      }

      def getNumberOfSomes(commits: List[Commit]): Int = {
        commits match {
          case Nil => 0
          case i :: tail =>
            i.stats match {
              case None => getNumberOfSomes(tail)
              case x => 1 + getNumberOfSomes(tail)
            }
        }
      }

      getSum(input) / getNumberOfSomes(input)*/
    }

  /** Q24 (4p)
   * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
   * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
   * NB!filename of a file is always defined.
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   *
   * @param input list of commits to process.
   * @return the hour and the amount of files changed during this hour.
   */
  def jsTime(input: List[Commit]): (Int, Int) = {
    if (input == Nil) return (0, 0)
    val dateFormat = new SimpleDateFormat("HH")
    dateFormat.setTimeZone(new SimpleTimeZone(0, "0"))
    val timeFiles = input.map(Commit => (dateFormat.format(Commit.commit.committer.date).toInt,
      Commit.files.flatMap(File => File.filename).count(s => s.endsWith(".js"))))

    def hours(inp: List[(Int, Int)], hour: Int): Int = (inp, hour) match {
      case (Nil,_) => 0
      case (i :: tail,hr) => if (i._1 == hr) i._2 + hours(tail, hr) else 0
    }
    def loopAllHours(hour: Int): List[Int] = hour match {
      case 24 => Nil
      case i => List(hours(timeFiles, i)) ::: loopAllHours(i+1)
    }
    val hoursList = loopAllHours(0)
    val maxCount = hoursList.max
    val maxIndex = hoursList.indexOf(maxCount)
    (maxIndex, maxCount)

    /*def getHour(commit: Commit): Int = {
      commit.commit.committer.date.getHours // returns the hour in 24-hour format
    }

    val hourToJsFileCount: Map[Int, Int] = input
      .flatMap(commit => {
        // Extract the hour of the commit
        val hour = getHour(commit)

        // Filter out .js files and map each to the hour
        val someFiles = commit.files.filter(file => file.filename.isDefined)
        val jsFiles = someFiles.filter(file => file.filename.takeRight(3) == ".js")

        // For each JS file, return the hour associated with it
        jsFiles.map(_ => hour)
      })
      // Group by hour and count the number of JS files changed in each hour
      .groupBy(identity)
      .view.mapValues(_.size)
      .toMap

    // Step 2: Find the hour with the maximum number of JS file changes
    hourToJsFileCount.maxBy(_._2)*/
  }


  /** Q25 (5p)
   * For a given repository, output the name and amount of commits for the person
   * with the most commits to this repository.
   * For the name, use `commit.commit.author.name`.
   *
   * @param input the list of commits to process.
   * @param repo  the repository name to consider.
   * @return the name and amount of commits for the top committer.
   */
  def topCommitter(input: List[Commit], repo: String): (String, Int) = ???

  /** Q26 (9p)
   * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
   * Leave out all repositories that had no activity this year.
   *
   * @param input the list of commits to process.
   * @return a map that maps the repo name to the amount of commits.
   *
   *         Example output:
   *         Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
   */
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = ???
    /*val only2019 = input.filter(commit => commit.commit.committer.date.getYear == 119)
    def helper(input: List[Commit]): Map[String, Int] = {
      input match {
        case Nil =>  Map.empty[String, Int]
        case i :: tail =>
          i.
      }
    }*/


  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = ???


  /** Q28 (9p)
   *
   * A day has different parts:
   * morning 5 am to 12 pm (noon)
   * afternoon 12 pm to 5 pm.
   * evening 5 pm to 9 pm.
   * night 9 pm to 4 am.
   *
   * Which part of the day was the most productive in terms of commits ?
   * Return a tuple with the part of the day and the number of commits
   *
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   */
  def mostProductivePart(input: List[Commit]): (String, Int) = ???
}
