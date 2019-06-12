package httpWrapper

// ====== Imports for Scala WS Client
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import play.api.libs.ws._
import play.api.libs.ws.ahc._
import DefaultBodyReadables._
import DefaultBodyWritables._
import scala.concurrent.ExecutionContext.Implicits._
import scala.util.{Try, Success, Failure}
import java.util.concurrent.{ TimeoutException, CountDownLatch, TimeUnit }
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
// ====== End of Imports for Scala WS Client

class sapSession(url: String, user: String, password: String) {

  var wsClient:StandaloneWSClient = _
  var token:String = _
  var cookies:Seq[play.api.libs.ws.WSCookie] = _

  def init() = {
  
    // Create Akka system for thread and streaming management
    implicit val system = ActorSystem()
    system.registerOnTermination {
            System.exit(0)
         }
    
    implicit val materializer = ActorMaterializer()
    wsClient = StandaloneAhcWSClient()
  }
  
  def call_get(): Unit = {
    
    val fResponse = wsClient.url(url)
      .withAuth(user, password, WSAuthScheme.BASIC)
    .addHttpHeaders(("X-CSRF-TOKEN","Fetch"))
      .get()
      
      val response = Await.result(fResponse, 4 seconds)
      
      val statusText: String = response.statusText
      val body = response.body[String]
           
      token = response.headers("x-csrf-token")(0).toString
      cookies = response.cookies
      
      println("token: " + token) 
  }
    
  def call_post_sync(reqBody: String): String = {
      
      var req = wsClient.url(url)
      .withAuth(user, password, WSAuthScheme.BASIC)
      .addHttpHeaders(("x-csrf-token", token))
      
      val fResponse = cookies.foldLeft(req) { (r,c) => r.addCookies(c) }
      .post(reqBody)
      val response = Await.result(fResponse, 2 seconds) 
      val statusText: String = response.statusText
      val body = response.body[String]
      body
  }
  
  def call_post_async(reqBody: String): Unit = {
      
      var req = wsClient.url(url)
      .withAuth(user, password, WSAuthScheme.BASIC)
      .addHttpHeaders(("x-csrf-token", token))
      
      cookies.foldLeft(req) { (r,c) => r.addCookies(c) }
      .post(reqBody)
      
  } // of call_post
  
  
  } // of Class