package org.esgi.project

import java.time.Instant
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
import org.apache.kafka.streams.state.{KeyValueIterator, QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore, WindowStoreIterator}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.esgi.project.models.{Error, Like, Movie, MovieLike, MovieLikes, MovieStats, Stat, TopLikes, TopViews, View}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.util.Try

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
  val movieLikesGroupedByMovieStoreName: String = s"movieLikesGroupedByMovie-$randomUuid"
  val viewsGroupedByMovieFullStoreName: String = s"viewsGroupedByMovie-$randomUuid"
  val viewsGroupedByMovie1minStoreName: String = s"viewsGroupedByMovie1min-$randomUuid"
  val viewsGroupedByMovie5minStoreName: String = s"viewsGroupedByMovie5min-$randomUuid"

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

    val viewsLikesStream = viewsStream
      .join(likesStream)(
        (view: View, like: Like) => {
          MovieLike(title = view.title, score = like.score)
        },
        JoinWindows.of(30.seconds.toMillis)
      )(Joined.`with`(Serdes.String, View.serdes, Like.serdes))

    // Views grouped by movie
    val viewsGroupedByMovie: KGroupedStream[String, View] = viewsStream.groupByKey(Serialized.`with`(Serdes.String, View.serdes))

    // View likes grouped by movie
    val viewsLikesGroupedByMovie: KGroupedStream[String, MovieLike] = viewsLikesStream.groupByKey(Serialized.`with`(Serdes.String, MovieLike.serdes))
    viewsLikesGroupedByMovie.aggregate(MovieLikes()
    )((_: String, v: MovieLike, ml: MovieLikes) => {
      ml.copy(title = v.title, movieLikesCount = ml.movieLikesCount + 1, score = ml.score + v.score)
    })(Materialized.as(movieLikesGroupedByMovieStoreName).withValueSerde(MovieLikes.serdes))

    val viewsGroupedByMovie1min: TimeWindowedKStream[String, View] = viewsGroupedByMovie
      .windowedBy(TimeWindows.of(1.minute.toMillis).advanceBy(10.second.toMillis))

    val viewsGroupedByMovie5min: TimeWindowedKStream[String, View] = viewsGroupedByMovie
      .windowedBy(TimeWindows.of(5.minute.toMillis).advanceBy(1.minute.toMillis))

    def updateMoviePastStat(view: View, movie: Movie): Movie = {
      view.viewCategory match {
        case "start_only" => {
          movie.copy(id = view.id, title = view.title, viewCount = movie.viewCount + 1, stats = movie.stats.copy(past = movie.stats.past.copy(startOnly = movie.stats.past.startOnly + 1)))
        }
        case "half" => {
          movie.copy(id = view.id, title = view.title, viewCount = movie.viewCount + 1, stats = movie.stats.copy(past = movie.stats.past.copy(half = movie.stats.past.half + 1)))
        }
        case "full" => {
          movie.copy(id = view.id, title = view.title, viewCount = movie.viewCount + 1, stats = movie.stats.copy(past = movie.stats.past.copy(full = movie.stats.past.full + 1)))
        }
      }
    }

    def updateStat(view: View, stat: Stat): Stat = {
      view.viewCategory match {
        case "start_only" => {
          stat.copy(startOnly = stat.startOnly + 1)
        }
        case "half" => {
          stat.copy(half = stat.half + 1)
        }
        case "full" => {
          stat.copy(full = stat.full + 1)
        }
      }
    }

    viewsGroupedByMovie
      .aggregate(
        Movie(id = 0, title = "", viewCount = 0, stats = MovieStats(past = Stat(), lastMinute = Stat(), lastFiveMinutes = Stat()))
      )((_, v, m) => updateMoviePastStat(v, m))(Materialized.as(viewsGroupedByMovieFullStoreName).withValueSerde(Movie.serdes))

    viewsGroupedByMovie1min
      .aggregate(
        Stat()
      )((_, v, s) => updateStat(v, s))(Materialized.as(viewsGroupedByMovie1minStoreName).withValueSerde(Stat.serdes))

    viewsGroupedByMovie5min
      .aggregate(
        Stat()
      )((_, v, s) => updateStat(v, s))(Materialized.as(viewsGroupedByMovie5minStoreName).withValueSerde(Stat.serdes))

    builder.build()
  }


  def routes(): Route = {
    concat(
      path("movies" / IntNumber) { id: Int =>
        get {
          val viewsGroupedByMovieCountStore: ReadOnlyKeyValueStore[String, Movie] = streams.store(viewsGroupedByMovieFullStoreName, QueryableStoreTypes.keyValueStore[String, Movie]())
          val movie = viewsGroupedByMovieCountStore.get(id.toString)

          movie match {
            case movie: Movie => {
              val toTime = Instant.now().toEpochMilli
              val viewsGroupedByMovie1minStore: ReadOnlyWindowStore[String, Stat] = streams.store(viewsGroupedByMovie1minStoreName, QueryableStoreTypes.windowStore[String, Stat]())
              val viewsGroupedByMovie5minStore: ReadOnlyWindowStore[String, Stat] = streams.store(viewsGroupedByMovie5minStoreName, QueryableStoreTypes.windowStore[String, Stat]())

              val movieStat1minStoreWindowStores: WindowStoreIterator[Stat] = viewsGroupedByMovie1minStore.fetch(movie.id.toString, toTime - 1.minute.toMillis, toTime)
              val movieStats1min = Try(movieStat1minStoreWindowStores.asScala.toList.last.value).getOrElse(Stat())

              val movieStat5minStoreWindowStores: WindowStoreIterator[Stat] = viewsGroupedByMovie5minStore.fetch(movie.id.toString, toTime - 5.minute.toMillis, toTime)
              val movieStats5min = Try(movieStat5minStoreWindowStores.asScala.toList.last.value).getOrElse(Stat())

              complete(movie.copy(stats = movie.stats.copy(lastMinute = movieStats1min, lastFiveMinutes = movieStats5min)))
            }
            case _ => complete((404, Error(message = s"Movie $id not found")))
          }
        }
      },
      path("stats" / "ten" / Segment / "views") {
        segment: String => {
          get {
            val viewsGroupedByMovieCountStore: ReadOnlyKeyValueStore[String, Movie] = streams.store(viewsGroupedByMovieFullStoreName, QueryableStoreTypes.keyValueStore[String, Movie]())
            val movieIterator: KeyValueIterator[String, Movie] = viewsGroupedByMovieCountStore.all()
            val sorted: List[TopViews] = movieIterator.asScala.toList.map(x => TopViews(title = x.value.title, views = x.value.viewCount)).sortBy(_.views)
            segment match {
              case "best" => complete(sorted.reverse.take(10))
              case "worst" => complete(sorted.take(10))
              case _ => complete(404, Error(message = s"Incorrect type (got $segment, must be best or worst)"))
            }
          }
        }
      },
      path("stats" / "ten" / Segment / "score") {
        segment: String => {
          get {
            val likesGroupedByMovieStore: ReadOnlyKeyValueStore[String, MovieLikes] = streams.store(movieLikesGroupedByMovieStoreName, QueryableStoreTypes.keyValueStore[String, MovieLikes]())
            val movieIterator: KeyValueIterator[String, MovieLikes] = likesGroupedByMovieStore.all()
            val sorted: List[TopLikes] = movieIterator.asScala.toList.map(x => {
              TopLikes(title = x.value.title, score = x.value.score / x.value.movieLikesCount)
            }).sortBy(_.score)
            segment match {
              case "best" => complete(sorted.reverse.take(10))
              case "worst" => complete(sorted.take(10))
              case _ => complete(404, Error(message = s"Incorrect type (got $segment, must be best or worst)"))
            }
          }
        }
      }
    )
  }

  def main(args: Array[String]) {
    Http().bindAndHandle(routes(), "0.0.0.0", 8080)
    logger.info(s"App started on 8080")
  }
}
