package org.esgi.project

import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.kstream.{KGroupedStream => _, KStream => _, KTable => _, _}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{TimeWindowedKStream, _}
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.models.{Error, Like, Movie, MovieStat, View}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

object Main extends PlayJsonSupport {

  import Serdes._

  implicit val system: ActorSystem = ActorSystem.create("this-system")
  implicit val materializer: ActorMaterializer = ActorMaterializer.create(system)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val config: Config = ConfigFactory.load()

  val props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-esgi-project")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    p
  }

  // Store names
  val randomUuid: String = UUID.randomUUID.toString
  val viewsGroupedByMovieCountStoreName: String = s"viewsGroupedByMovie-$randomUuid"
  val viewsGroupedByMovieAndViewCategory1minStoreName: String = s"viewsGroupedByMovieAndViewCategory1min-$randomUuid"
  val viewsGroupedByMovieAndViewCategory5minStoreName: String = s"viewsGroupedByMovieAndViewCategory5min-$randomUuid"

  val streams: KafkaStreams = new KafkaStreams(buildProcessingGraph, props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close()
  }

  def buildProcessingGraph: Topology = {
    import Serdes._

    val builder: StreamsBuilder = new StreamsBuilder

    // TODO: check error on parsing
    val viewsStream: KStream[String, View] = builder.stream[String, String]("views")
      .mapValues(value => Json.parse(value).as[View])

    val likesStream: KStream[String, Like] = builder.stream[String, String]("likes")
      .mapValues(value => Json.parse(value).as[Like])

    // Views grouped by movie
    val viewsGroupedByMovie: KGroupedStream[String, View] = viewsStream.groupByKey(Serialized.`with`(Serdes.String, View.serdes))

    //     Views grouped by movie and view category
    val viewsGroupedByMovieAndViewCategory: KGroupedStream[String, View] = viewsStream
      .selectKey((k, v) => k + '-' + v.viewCategory).groupByKey(Serialized.`with`(Serdes.String, View.serdes))

    val viewsGroupedByMovieAndViewCategory1min: TimeWindowedKStream[String, View] = viewsGroupedByMovieAndViewCategory
      .windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(10.second.toMillis))

    val viewsGroupedByMovieAndViewCategory5min: TimeWindowedKStream[String, View] = viewsGroupedByMovieAndViewCategory
      .windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.minute.toMillis))

    def updateMovieStat(view: View, movie: Movie): Movie = {
      view.viewCategory match {
        case "start_only" => {
          movie.copy(id = view.id, title = view.title, viewCount = movie.viewCount + 1, stat = movie.stat.copy(startOnly = movie.stat.startOnly + 1))
        }
        case "half" => {
          movie.copy(id = view.id, title = view.title, viewCount = movie.viewCount + 1, stat = movie.stat.copy(half = movie.stat.half + 1))
        }
        case "full" => {
          movie.copy(id = view.id, title = view.title, viewCount = movie.viewCount + 1, stat = movie.stat.copy(full = movie.stat.full + 1))
        }
      }
    }

    viewsGroupedByMovie
      .aggregate(
        Movie(id = 0, title = "", viewCount = 0, stat = MovieStat())
      )((_, v, m) => updateMovieStat(v, m))(Materialized.as(viewsGroupedByMovieCountStoreName).withValueSerde(Movie.serdes))

    viewsGroupedByMovieAndViewCategory1min
      .aggregate(
        Movie(id = 0, title = "", viewCount = 0, stat = MovieStat())
      )((_, v, m) => updateMovieStat(v, m))(Materialized.as(viewsGroupedByMovieCountStoreName).withValueSerde(Movie.serdes))

    viewsGroupedByMovieAndViewCategory5min
      .aggregate(
        Movie(id = 0, title = "", viewCount = 0, stat = MovieStat())
      )((_, v, m) => updateMovieStat(v, m))(Materialized.as(viewsGroupedByMovieCountStoreName).withValueSerde(Movie.serdes))


    builder.build()
  }


  val route: Route =
    path("movies" / IntNumber) { id: Int =>
      get {
        val viewsGroupedByMovieCountStore = streams.store(viewsGroupedByMovieCountStoreName, QueryableStoreTypes.keyValueStore[String, Movie]())
        val movie = viewsGroupedByMovieCountStore.get(id.toString)

        movie match {
          case movie: Movie => complete(movie)
          case _ => complete((404, Error(message = s"Movie $id not found")))
        }
      }
    }


  def main(args: Array[String]) {
    Http().bindAndHandle(route, "0.0.0.0", 8080)
    logger.info(s"App started on 8080")
  }
}
