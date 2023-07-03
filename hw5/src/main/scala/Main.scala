import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source, ZipWith}
import akka.stream.{ClosedShape, Graph}

object Main {
//  Create ActorSystem - main Akka component
  implicit val system: ActorSystem = ActorSystem("HomeWork_5")
//  Create GraphDSL that determine graph structure for stream data handling
  val graph: Graph[ClosedShape.type, NotUsed] = GraphDSL.create() {
    implicit builder: GraphDSL.Builder[NotUsed] =>
      import GraphDSL.Implicits._
//  Create graph elements
//  input - data source generating integers from 1 to 5
      val input = builder.add(Source(1 to 5))
//  create 3 multipliers
      val multiplier10 = builder.add(Flow[Int].map(x => x * 10))
      val multiplier2 = builder.add(Flow[Int].map(x => x * 2))
      val multiplier3 = builder.add(Flow[Int].map(x => x * 3))
//  output - data consumer
      val output = builder.add(Sink.foreach(println))
//  broadcast - delimiter of one stream on three separate streams
      val broadcast = builder.add(Broadcast[Int](3))
//  zipWith collects data from streams to corteges
      val zipWith = builder.add(ZipWith[Int, Int, Int, (Int, Int, Int)]((a, b, c) => (a, b, c)))
//  operator ~> connects graph elements
//  values from input are sent to broadcast
      input ~> broadcast
//  from broadcast values are sent to multipliers
      broadcast.out(0) ~> multiplier10 ~> zipWith.in0
      broadcast.out(1) ~> multiplier2 ~> zipWith.in1
      broadcast.out(2) ~> multiplier3 ~> zipWith.in2
//  after multipliers values are collected by operator "out" of zipWith and then sent to output
      zipWith.out ~> output
//  End of graph description. ClosedShape means that all elements should be connected
      ClosedShape
  }

  def main(args: Array[String]): Unit = {
//    Run of graph "graph"
    RunnableGraph.fromGraph(graph).run()
  }
}