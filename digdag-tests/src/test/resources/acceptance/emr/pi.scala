import scala.math.random
val slices = 2
val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
val count = sc.parallelize(1 until n, slices).map { i =>
  val x = random * 2 - 1
  val y = random * 2 - 1
  if (x*x + y*y <= 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / (n - 1))
