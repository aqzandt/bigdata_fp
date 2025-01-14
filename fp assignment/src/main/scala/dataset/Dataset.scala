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
    val dateFormat = new SimpleDateFormat("HH")
    dateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"))
    val timeFiles = input.map(Commit => (dateFormat.format(Commit.commit.committer.date).toInt,
      Commit.files.flatMap(File => File.filename).count(s => s.endsWith(".js"))))
    def hours(inp: List[(Int, Int)], hour: Int): Int = (inp, hour) match {
      case (Nil,_) => 0
      case (i :: tail,hr) => if (i._1 == hr) i._2 + hours(tail, hr) else hours(tail, hr)
    }
    def loopAllHours(hour: Int): List[Int] = hour match {
      case 24 => Nil
      case hr => List(hours(timeFiles, hr)) ::: loopAllHours(hr+1)
    }
    val hoursList = loopAllHours(0)
    val maxCount = hoursList.max
    val maxIndex = hoursList.indexOf(maxCount)
    (maxIndex, maxCount)
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
  def topCommitter(input: List[Commit], repo: String): (String, Int) = {
    val list = input.map(Commit => (Commit.commit.author.name, Commit.url.substring(29,Commit.url.length-49)))
    val authorRepoCount = list.groupBy(identity).map(i => (i._1, i._2.length)).toList.sortBy(i => i._2).reverse
    val repoList = authorRepoCount.filter(i => i._1._2.equals(repo))
    (repoList.head._1._1,repoList.head._2)
  }

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
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
    val dateFormat = new SimpleDateFormat("YYYY")
    dateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"))
    input.map(Commit => (Commit.url.substring(29,Commit.url.length-49),dateFormat.format(Commit.commit.committer.date)))
      .filter(i => i._2.equals("2019")).map(i => i._1).groupBy(identity).map(i => (i._1, i._2.length))
  }

  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = {
    val sortedList = input.flatMap(Commit => Commit.files).flatMap(file => file.filename).map(s => s.substring(s.lastIndexOf('.')+1))
      .groupBy(identity).map(i => (i._1, i._2.length)).toList.sortBy(i => i._2).reverse
    sortedList.slice(0,5)
  }


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
  def mostProductivePart(input: List[Commit]): (String, Int) = {
    val dateFormat = new SimpleDateFormat("HH")
    dateFormat.setTimeZone(new SimpleTimeZone(0, "UTC"))
    val func = (i:Int) => if (i >= 21 || i < 5) "night" else if (i >= 5 && i < 12) "morning" else if (i >=12 && i < 17) "afternoon" else "evening"
    input.map(Commit => dateFormat.format(Commit.commit.committer.date).toInt).map(func).groupBy(identity).map(i => (i._1, i._2.length))
      .toList.sortBy(i => i._2).reverse.head
  }
}
