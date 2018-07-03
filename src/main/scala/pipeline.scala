import cats.data._
import cats.effect.IO
import cats._
import cats.implicits._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

trait Pipeline[A,B] {
  type Result[I,O] = ReaderT[IO,I,O]

  def read(path: String): ReaderT[IO, SparkSession, Dataset[A]] = {
    ReaderT(spark => IO { spark.read.csv(path).as[A] })
  }

  def transform(f: A=>B): Result[Dataset[A], Dataset[B]] = {
    ReaderT(fa => IO { fa.map(f) })
  }

  def write(path: String): Result[Dataset[B], Unit] = {
    ReaderT(fb => IO { fb.write.json(path) })
  }

  def program[A,B](input: String, f: A=>B,  output: String)(implicit spark: SparkSession): IO[Unit] = for {
    r <- read(input)(spark)
    t <- transform(f)(r)
    w <- write(output)(t)
  } yield w
}

// Example instance

case class User(name: String, age: Int)

object MyPipeline extends Pipeline[User, String] {
  implicit val spark = SparkSession
    .builder()
    .appName("SparkSessionExample")
    .setMaster("local")
    .getOrCreate()


}

// When you actually want the effect
MyPipeline.program("inputPath", (u:User)=> u.name, "outputPath").unsafeRunSync()