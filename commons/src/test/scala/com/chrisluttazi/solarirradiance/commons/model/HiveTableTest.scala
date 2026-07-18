package com.chrisluttazi.solarirradiance.commons.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HiveTableTest extends AnyFlatSpec with Matchers {

  private def withDataRoot[A](root: String)(body: => A): A = {
    val previous = sys.props.get(RootPath.SysProp)
    sys.props(RootPath.SysProp) = root
    try body
    finally
      previous match {
        case Some(value) => sys.props(RootPath.SysProp) = value
        case None        => sys.props -= RootPath.SysProp
      }
  }

  "RootPath" should "resolve the dataset root from the system property" in {
    withDataRoot("/data/sets") {
      stationsRoot.toString shouldBe "/data/sets/Stations"
      weatherRoot.toString shouldBe "/data/sets/Weather"
    }
  }

  it should "fail with a clear error when no dataset root is configured" in {
    val previous = sys.props.get(RootPath.SysProp)
    sys.props -= RootPath.SysProp
    try {
      if (sys.env.contains(RootPath.EnvVar)) {
        cancel(s"${RootPath.EnvVar} is set in this environment")
      }
      val error = the[IllegalStateException] thrownBy new RootPath("/x").toString
      error.getMessage should include(RootPath.EnvVar)
      error.getMessage should include(RootPath.SysProp)
    } finally {
      previous.foreach(value => sys.props(RootPath.SysProp) = value)
    }
  }

  "HiveTable" should "emit the skip-header table property only when headers are ignored" in {
    withDataRoot("/data/sets") {
      stationsData.header should include("skip.header.line.count")
      stations.header shouldBe ""
    }
  }
}
