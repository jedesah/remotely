import scalaz.{-\/, \/, \/-}

//: ----------------------------------------------------------------------------
//: Copyright (C) 2014 Verizon.  All Rights Reserved.
//:
//:   Licensed under the Apache License, Version 2.0 (the "License");
//:   you may not use this file except in compliance with the License.
//:   You may obtain a copy of the License at
//:
//:       http://www.apache.org/licenses/LICENSE-2.0
//:
//:   Unless required by applicable law or agreed to in writing, software
//:   distributed under the License is distributed on an "AS IS" BASIS,
//:   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//:   See the License for the specific language governing permissions and
//:   limitations under the License.
//:
//: ----------------------------------------------------------------------------


package object remotely {
  import scala.concurrent.duration._
  import scala.reflect.runtime.universe.TypeTag
  import scalaz.stream.{Process,Process1, Cause}
  import Cause.End
  import Process.Halt
  import scalaz.concurrent.Task
  import scalaz.\/.{left,right}
  import scalaz.Monoid
  import scodec.bits.{BitVector,ByteVector}
  import scodec.Decoder
  import scalaz.std.anyVal._
  import scalaz.std.tuple._

/**
  * Represents the logic of a connection handler, a function
  * from a stream of bytes to a stream of bytes, which will
  * be sent back to the client. The connection will be closed
  * when the returned stream terminates.
  *
  * NB: This type cannot represent certain kinds of 'coroutine'
  * client/server interactions, where the server awaits a response
  * to a particular packet sent before continuing.
  */
  type Handler = Process[Task,BitVector] => Process[Task,BitVector]

  /**
   * Evaluate the given remote expression at the
   * specified endpoint, and get back the result.
   * This function is completely pure - no network
   * activity occurs until the returned `Response` is
   * run.
   *
   * The `Monitoring` instance is notified of each request.
   */
  def evaluate[A:Decoder:TypeTag](e: Endpoint, M: Monitoring = Monitoring.empty)(r: Remote[A]): Response[A] =
    evaluateImpl(e,M)(r)

  /**
   * Evaluate the given remote stream at the
   * specified endpoint, and get back the result.
   * This function is completely pure - no network
   * activity occurs until the returned `Response` is
   * run.
   *
   * The `Monitoring` instance is notified of each request.
   */
  def evaluateStream[A:Decoder:TypeTag](e: Endpoint, M: Monitoring = Monitoring.empty)(r: Remote[Process[Task, A]]): Response[A] =
    evaluateImpl(e,M)(r)

  /**
   * In the actual implementation, we don't really care what type of Remote we receive. In any case, it is
   * going to become a stream of bits and produce wtv is the expected result is there are no errors.
   */
  private def evaluateImpl[A: Decoder:TypeTag](e: Endpoint, M: Monitoring = Monitoring.empty)(r: Remote[Any]): Response[A] =
    Remote.localize(r).flatMap { r => Response.scope { Response { ctx => // push a fresh ID onto the call stack
      val refs = Remote.refs(r)

      def reportErrors[R](startNanos: Long)(t: Process[Task,R]): Process[Task,R] =
        t.onFailure{e =>
          M.handled(ctx,r,refs, left(e), Duration.fromNanos(System.nanoTime - startNanos))
          Process.empty[Task,R]
        }

      def failOnServerSideErr[A](startNanos: Long)(p: Process[Task, String \/ A]): Process[Task,A] =
        p.flatMap{
          case -\/(error:String) =>
            // TODO: Error decoding response??????!!!!!
            val ex = ServerException("error decoding response: " + error)
            val delta = System.nanoTime - startNanos
            M.handled(ctx, r, Remote.refs(r), left(ex), Duration.fromNanos(delta))
            Process.fail(ex)
          case \/-(a) =>
            val delta = System.nanoTime - startNanos
            M.handled(ctx, r, refs, right(a), Duration.fromNanos(delta))
            Process.emit(a)
        }

      val startTime = Task.delay { System.nanoTime() }
      Process.await(startTime.flatMap(start => e.get.map(conn => (start,conn)))) { case (start, conn) =>
        val reqBits = codecs.encodeRequest(r).apply(ctx)
        val respBytes = reportErrors(start) { conn(reqBits) }
        val response = reportErrors(start) { codecs.decodeResponse(respBytes) }
        failOnServerSideErr(start)(response)
      }
    }
    }}

  implicit val BitVectorMonoid = Monoid.instance[BitVector]((a,b) => a ++ b, BitVector.empty)
  implicit val ByteVectorMonoid = Monoid.instance[ByteVector]((a,b) => a ++ b, ByteVector.empty)

  private[remotely] def fullyRead(s: Process[Task,BitVector]): Task[BitVector] = s.runFoldMap(x => x)
}
