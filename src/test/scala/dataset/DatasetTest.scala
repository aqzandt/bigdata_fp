package dataset

import Dataset._
import dataset.util.Commit.Commit
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.scalatest.FunSuite

import scala.io.Source

class DatasetTest extends FunSuite {

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val source: List[Commit] = Source.fromResource("1000_commits.json").getLines().map(Serialization.read[Commit]).toList
    val jsTimeSource: List[Commit] = Source.fromResource("jsTimeTest.json").getLines().map(Serialization.read[Commit]).toList

    test("Average additions"){
        assertResult(3137) {
            avgAdditions(source)
        }
    }

    test("Time of day javascript") {
        assertResult((12, 830)) {
            jsTime(source)
        }
    }

    test("jsTimeTest") {
        assertResult((0, 3)) {
            jsTime(jsTimeSource)
        }
    }

    test("Top committer") {
        assertResult(("Leonid Plyushch", 12)) {
            topCommitter(source, "termux/termux-packages")
        }
    }

    test("Top committer 2") {
        assertResult(("Johannes", 22)) {
            topCommitter(source, "keptn-deploy/sockshop")
        }
    }


    test("Commits per repo") {
        assertResult(read[Map[String, Int]](Source.fromResource("commits_per_repo.json").mkString)) {
            commitsPerRepo(source)
        }
    }

    test("Top languages") {
        assertResult(List(("html",910), ("js",848), ("json",554), ("png",434), ("md",408))) {
            topFileFormats(source)
        }
    }

    test("Most productive") {
        assertResult(("afternoon", 992)) {
            mostProductivePart(source)
        }
    }
}
