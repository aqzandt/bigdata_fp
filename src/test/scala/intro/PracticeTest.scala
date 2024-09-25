package intro

import Practice._
import org.scalatest.FunSuite

class PracticeTest extends FunSuite {

    test("FirstN") {
        assertResult((1 to 10).toList) {
            firstN((1 to 20).toList, 10)
        }
    }

    test("MaxValue") {
        assertResult(16) {
            maxValue(List(10, 4, 14, -4, 15, 14, 16, 7))
        }
    }

    test("intList") {
        assertResult(List(5,6,7)) {
            intList(5,7)
        }
    }

    test("funcTest") {
        val nrs = List.range(0,11) // List(0,1,2,3,...,10)
         // List(0,4,8)
        assertResult(List(0,4,8)) {
            myFilter(nrs, (i: Int) => i % 2 == 0)
        }
    }
}
