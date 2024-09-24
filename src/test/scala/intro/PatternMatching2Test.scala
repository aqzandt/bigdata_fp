package intro

import org.scalatest.FunSuite
import intro.PatternMatching2._

class PatternMatching2Test extends FunSuite {


  test(" twice") {
    assertResult(List("abc", "abc")) {
      twice(List("abc"))
    }
  }

  test("drunkWords") {
    assertResult(List("uoy", "yeh")) {
      drunkWords(List("hey", "you"))
    }
  }

  test("myForAll") {
    assertResult(true) {
      myForAll(List("hey"), (s: String) => s.startsWith("h"))
    }
  }

  test("myForAllEmpty") {
    assertResult(true) {
      myForAll(List(), (s: String) => s.startsWith("h"))
    }
  }

  test("myForAllFalse") {
    assertResult(false) {
      myForAll(List("hey"), (s: String) => s.startsWith("s"))
    }
  }

  test("myForAllMultipleTrue") {
    assertResult(true) {
      myForAll(List("hey", "hi"), (s: String) => s.startsWith("h"))
    }
  }

  test("myForAllMultipleFalse") {
    assertResult(false) {
      myForAll(List("hey", "si", "hi"), (s: String) => s.startsWith("h"))
    }
  }

  test("lastElem") {
    assertResult(Some("yes")) {
      lastElem(List("no", "yes", "no", "no", "yes"))
    }
  }

  test("append") {
    assertResult(List(1, 3, 5, 2, 4)) {
      append(List(1, 3, 5), List(2, 4))
    }
  }
}
