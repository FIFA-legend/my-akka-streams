package part2_primer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

object FirstPrinciples extends App {

  /*
    Concepts:
    - publisher = emits elements (asynchronously)
    - subscriber = receives elements
    - processor = transforms elements along the way
    - async
    - backpressure
   */

  /*
    Akka Streams

    Source = "publisher"
    - emits elements asynchronously
    - may or may not terminate

    Sink = "subscriber"
    - receives elements
    - terminates only when the publisher terminates

    Flow = "processor"
    - transforms elements

    Directions
    - upstream = to the source
    - downstream = to the sink
   */

  implicit val system = ActorSystem("FirstPrinciples")
  implicit val materializer = ActorMaterializer() // allows running of Akka Stream components

  // sources
  val source = Source(1 to 10)
  // sinks
  val sink = Sink.foreach[Int](println)

  val graph = source.to(sink)
  // graph.run()

  // flows transform elements
  val flow = Flow[Int].map(x => x + 1)
  val sourceWithFlow = source.via(flow)
  val flowWithSink = flow.to(sink)

//  sourceWithFlow.to(sink).run()
//  source.to(flowWithSink).run()
//  source.via(flow).to(sink).run()

  // nulls are NOT allowed
  // values must be IMMUTABLE and SERIALIZABLE
  // val illegalSource = Source.single[String](null) // throws NullPointerException
  // illegalSource.to(Sink.foreach(println)).run()
  // use Options instead

  // various kinds of sources
  val finiteSource = Source.single(1)
  val anotherFiniteSource = Source(List(1, 2, 3))
  val emptySource = Source.empty[Int]
  val infiniteSource = Source(Stream.from(1)) // do not confuse an Akka stream with a "collection" Stream

  import scala.concurrent.ExecutionContext.Implicits.global
  val futureSource = Source.fromFuture(Future(42))

  // sinks
  val theMostBoringSink = Sink.ignore // consumes everything and does nothing
  val foreachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int] // retrieves head ane then closes the stream
  val foldSink = Sink.fold[Int, Int](0)((a, b) => a + b)

  // flows - usually mapped to collection operators
  val mapFlow = Flow[Int].map(x => 2 * x)
  val takeFlow = Flow[Int].take(5)
  // drop, filter
  // NOT have flatMap

  // source -> flow -> flow -> ... -> sink
  val doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink)
  // doubleFlowGraph.run()

  // syntactic sugars
  val mapSource = Source(1 to 10).map(x => x * 2) // Source(1 to 10).via(Flow[Int].map(x => x * 2))
  // run streams directly
  // mapSource.runForeach(println) // mapSource.to(Sink.foreach[Int](println)).run()

  // OPERATORS = components

  /**
   * Exercise: create a stream that takes the names of persons, then you will keep the first 2 names with length > 5 characters
   */
  // Source(List("Nikita", "Name", "Petya", "Victoria")).filter(_.length > 5).take(2).runForeach(println)

  val names = List("Alice", "Bob", "Charlie", "David", "Martin", "AkkaStreams")
  val nameSource = Source(names)
  val longNameFlow = Flow[String].filter(name => name.length > 5)
  val limitFlow = Flow[String].take(2)
  val nameSink = Sink.foreach[String](println)

  nameSource.via(longNameFlow).via(limitFlow).to(nameSink).run()
  nameSource.filter(_.length > 5).take(2).runForeach(println)
}
