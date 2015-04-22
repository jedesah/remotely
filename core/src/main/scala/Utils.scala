package remotely

import scala.util.{Failure, Try, Success}
import scalaz.{\/-, -\/, \/}
import scalaz.concurrent.Task
import scalaz.stream.Process

package object utils {
  implicit class AugmentedProcess[A](p: Process[Task, A]) {
    def flatten[B](implicit conv: A => Process[Task,B]): Process[Task,B] =
      p.flatMap(conv)
  }

  implicit class AugmentedDisjunctionProcess[A, E](p: Process[Task, E \/ A]) {
    def flatten(conv: E => Throwable): Process[Task, A] = p.flatMap(dtoP(_)(conv))
  }

  def dtoP[E,A](a: E \/ A)(conv: E => Throwable): Process[Task, A] =
    a fold(
      e => Process.fail(conv(e)),
      a => Process.emit(a)
    )

  implicit def tryToProcess[A](t: Try[A]): Process[Task, A] = t match {
    case Failure(e) => Process.fail(e)
    case Success(a) => Process.emit(a)
  }
}