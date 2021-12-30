import org.scalatest.funsuite.AnyFunSuite

class TestCasesForSpark2 extends AnyFunSuite{

  assert(SparkAssignment2.linesContainRDDFile() == 281234)
}
