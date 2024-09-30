package fp_practice

import scala.annotation.tailrec

/**
  * In this part you can practice your FP skills with some small exercises.
  * Hint: you can find many useful functions in the documentation of the List class.
  *
  * This part is worth 15 points.
  */
object FPPractice {

    /** Q20 (4p)
      * Returns the sum of the first 10 numbers larger than 25 in the given list.
      * Note that 10 is an upper bound, there might be less elements that are larger than 25.
      *
      * @param xs the list to process.
      * @return the sum of the first 10 numbers larger than 25.
      */
    def first10Above25(xs: List[Int]): Int = {
        def helper(xs: List[Int], counter: Int): Int = {
            (xs, counter) match {
                case (Nil, _) => 0
                case (_, 10) => 0
                case (i :: tail, _) if i > 25 => i + helper(tail, counter + 1)
                case (i :: tail, _) => helper(tail, counter)
            }
        }
        helper(xs, 0)
    }

    /** Q21 (5p)
      * Provided with a list of all grades for each student of a course,
      * count the amount of passing students.
      * A student passes the course when the average of the grades is at least 5.75 and no grade is lower than 4.
      *
      * @param grades a list containing a list of grades for each student.
      * @return the amount of students with passing grades.
      */
    def passingStudents(grades: List[List[Int]]): Int = {
        def isPassing(grades: List[Int]): Boolean = {
            def getSum(grades: List[Int]): Int = {
                grades match {
                    case Nil => 0
                    case i :: tail => i + getSum(tail)
                }
            }

            @tailrec
            def hasOnlyHighGrades(grades: List[Int]): Boolean = {
                grades match {
                    case Nil => true
                    case i :: tail => (i >= 4) && hasOnlyHighGrades(tail)
                }
            }
            if(getSum(grades) >= 5.75 * grades.size && hasOnlyHighGrades(grades)) true
            else false
        }
        grades match {
            case Nil => 0
            case i :: tail =>
                if(isPassing(i)) 1 + passingStudents(tail)
                else passingStudents(tail)
        }
    }

    /** Q22 (6p)
      * Return the length of the first list of which the first item's value is equal to the sum of all other items.
      * @param xs the list to process
      * @return the length of the first list of which the first item's value is equal to the sum of all other items,
      *         or None if no such list exists.
      *
      * Read the documentation on the `Option` class to find out what you should return.
      * Hint: it is very similar to the `OptionalInt` you saw earlier.
      */
    @tailrec
    def headSumsTail(xs: List[List[Int]]): Option[Int] = {
        def getSum(xs: List[Int]): Int = {
            xs match {
                case Nil => 0
                case i :: tail => i + getSum(tail)
            }
        }
        xs match {
            case Nil => None
            case i :: tail =>
                if(getSum(i) == 2 * i.head) Option(i.size)
                else headSumsTail(tail)
        }
    }
}
