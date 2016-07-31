package com.biosimilarity.evaluator.spray

import akka.actor._
import com.biosimilarity.lift.lib._
import com.biosimilarity.evaluator.distribution._
import spray.routing._

import java.util.UUID

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._
import spray.http._
import spray.httpx.encoding._

import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

case class SetMessageId(id : String)
case class ApiMessageResponse(msgType : String, content : JObject)
case class CometMessage(data: String)


class ApiActor extends Actor with Serializable {

  var msgId : Option[String] = None
  def receive() = {
    case SetMessageId(id) => {
      msgId = Some(id)
    }
    case ApiMessageResponse(msgType,content) => {
      context.parent ! ApiResponse(msgType,msgId,content)
    }
    case _ => ()
  }

}

case class SessionPing(reqCtx: RequestContext) extends Serializable
case class PollTimeout() extends Serializable
case class SetSessionId(id : String) extends Serializable
case class KeepAlive() extends Serializable
case class RunFunction(fn : JObject => Unit, msgType : String, msgId : Option[String], content : JObject) extends Serializable
case class RunApiFunction(fn : (JObject, ActorRef) => Unit, msgType : String, msgId : Option[String], content : JObject) extends Serializable
case class CloseSession(reqCtx : Option[RequestContext]) extends Serializable
case class CameraItem(sending : Boolean, msgType : String, msgId : Option[String], content : Option[JObject], tmStamp : DateTime)
case class StartCamera(reqCtx : RequestContext)
case class StopCamera(reqCtx : RequestContext)
case class ApiResponse(msgType : String, msgId : Option[String], content : JObject)

class SessionActor extends Actor with Serializable {
  val gcTime = EvalConfConfig.readInt("sessionTimeoutMinutes") minutes // if client doesnt poll within this time, its garbage collected
  val clientTimeout = EvalConfConfig.readInt("pongTimeoutSeconds") seconds // ping requests are ponged after this much time, clients need to re-ping after this
  @transient
  var aliveTimer = context.system.scheduler.scheduleOnce(gcTime, self, CloseSession(None))
  @transient
  var optReq : Option[(RequestContext, Cancellable)] = None
  @transient
  var msgs : List[String] = Nil
  @transient
  var camera : Option[List[CameraItem]] = None
  var sessionId : Option[String] = None
  @transient
  implicit val formats = DefaultFormats

  override def postStop() = {
    sessionId = None
  }

  def resetAliveTimer() : Unit = {
    aliveTimer.cancel()
    aliveTimer = context.system.scheduler.scheduleOnce(gcTime, self, CloseSession(None))
  }

  def itemToString(itm : CameraItem) : String = {
    val cont : JValue = itm.content match {
      case Some(j) => j
      case None => JNull

    }
    var jo : JObject = "msgType" -> itm.msgType
    if (itm.msgId.isDefined) jo = jo ~ ("msgId" -> itm.msgId.get)
    jo = jo ~
      ("direction" -> (if (itm.sending) "Sent" else "Received") ) ~
      ("tmStamp" -> itm.tmStamp.toIsoDateTimeString) ~
      ("content" -> cont)
    pretty(render(jo))
  }

  def addCameraItem(itm : CameraItem) : Unit = {
    camera match {
      case Some(l) => camera = Some(itm :: l)
      case None => ()
    }
  }

  def itemReceived(msgType : String, msgId : Option[String], content : Option[JObject]) : Unit = {
    if (camera.isDefined) addCameraItem(CameraItem(sending = false, msgType, msgId, content, DateTime.now))
  }

  def itemSent(msgType : String, msgId : Option[String], content : Option[JObject]) : Unit = {
    if (camera.isDefined) addCameraItem(CameraItem(sending = true, msgType, msgId, content, DateTime.now))
  }

  def receive = {
    case SetSessionId(id) => {
      sessionId = Some(id)
      resetAliveTimer()
    }
    case KeepAlive() => {
      resetAliveTimer()
    }
    case SessionPing(reqCtx) => {
      itemReceived("sessionPing", None, None)
      for (ssn <- sessionId) {
        resetAliveTimer()
        for ((_, tmr) <- optReq) tmr.cancel()
        msgs match {
          case Nil => {
            optReq = Some(reqCtx, context.system.scheduler.scheduleOnce(clientTimeout, self, PollTimeout()))
          }
          case _ => {
            reqCtx.complete(HttpResponse(entity = "[" + msgs.reverse.mkString(",") + "]"))
            msgs = Nil
          }
        }
      }
    }
    case PollTimeout() => {
      for (ssn <- sessionId) {
        optReq match {
          case Some((req, _)) => {
            req.complete(HttpResponse(entity = compact(render(
              List(("msgType" -> "sessionPong") ~ ("content" -> ("sessionURI" -> ssn)))))))
            itemSent("sessionPong", None, None)
            optReq = None
          }
          case None => ()
        }
      }
    }
    case CloseSession(optReqCtx) => {
      context.stop(self)
      for (ssn <- sessionId) CometActorMapper.map -= ssn
      for (reqCtx <- optReqCtx) reqCtx.complete(StatusCodes.OK,"session closed")
    }
    case RunFunction(fn,msgType,msgId,content) => {
      itemReceived(msgType,msgId,Some(content))
      fn(content)
    }
    case RunApiFunction(fn,msgType,msgId,content) => {
      itemReceived(msgType,msgId,Some(content))
      val ch = context.actorOf(Props[ApiActor])
      if (msgId.isDefined) ch ! SetMessageId(msgId.get)
      fn(content,ch)
    }
    case ApiResponse(msgType,msgId,content) => {
      val msg = msgId match {
        case Some(id) => ("msgType" -> msgType) ~ ("msgId" -> id) ~ ("content" -> content)
        case None => ("msgType" -> msgType) ~ ("content" -> content)
      }
      self ! CometMessage(compact(render(msg)))
    }
    case CometMessage(data) => {
      msgs = data :: msgs
      resetAliveTimer()
      optReq match {
        case None => () //println("CometMessage optReqCtx miss: id = " + sessionId)
        case Some((reqCtx,tmr)) => {
          val rmsgs = msgs.reverse
          val now = DateTime.now
          rmsgs.foreach(msg => {
            val jo = parse(msg)
            val msgType = (jo \ "msgType").extract[String]
            val msgId = (jo \ "msgId").extract[Option[String]]
            val content = (jo \ "content").extract[JObject]
            addCameraItem( CameraItem(sending = true, msgType, msgId, Some(content), now) )
          })
          reqCtx.complete(HttpResponse(entity = "[" + rmsgs.mkString(",") + "]"))
          optReq = None
          msgs = Nil
          tmr.cancel()
        }
      }
    }
    case StartCamera(reqCtx) => {
      val msg : String = camera match {
        case Some(_) => "session is already recording"
        case None => "session recording started"
      }
      if (camera.isEmpty) camera = Some(Nil)
      itemReceived("startSessionRecording", None, None)
      reqCtx.complete(StatusCodes.OK,msg)
    }
    case StopCamera(reqCtx) => {
      itemReceived("stopSessionRecording", None, None)
      camera match {
        case None => reqCtx.complete(StatusCodes.PreconditionFailed,"session not recording" )
        case Some(itms) => {
          val items = itms.reverse.map(itemToString)
          reqCtx.complete(HttpResponse(entity = "[" + items.mkString(",") + "]"))
        }
      }
    }
  }
}


// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class EvaluatorServiceActor extends Actor
  with EvaluatorService
  with Serializable {
  import EvalHandlerService._

  // create well-known node user
  createNodeUser(NodeUser.email, NodeUser.password, NodeUser.jsonBlob)

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(myRoute)

  def actorRefFactory = context

}

case class InitializeSessionException(agentURI: String, message: String) extends Exception with Serializable

trait EvaluatorService extends HttpService
  with CORSSupport {
  import EvalHandlerService._

  def createNewSession(json: JObject, key: String) : Unit = {
    initializeSessionRequest(json, key, ssn => {
      val actor = actorRefFactory.actorOf(Props[SessionActor])
      actor ! SetSessionId(ssn)
      CometActorMapper.map += (ssn -> actor)
    })

  }

  @transient
  val syncMethods = HashMap[String, (JObject, String) => Unit](
    // Old stuff
    ("createUserRequest", createUserRequest),
    ("confirmEmailToken", confirmEmailToken),
    // New API
    ("createAgentRequest", createAgentRequest),
    ("initializeSessionRequest", createNewSession),
    ("getAgentRequest", getAgentRequest)
  )

  @transient
  val asyncMethods = HashMap[String, JObject => Unit](
    // Old stuff
    ("updateUserRequest", updateUserRequest),
    // Agents
    ("addAgentExternalIdentityRequest", addAgentExternalIdentityRequest),
    ("addAgentExternalIdentityToken", addAgentExternalIdentityToken),
    ("removeAgentExternalIdentitiesRequest", removeAgentExternalIdentitiesRequest),
    ("getAgentExternalIdentitiesRequest", getAgentExternalIdentitiesRequest),
    ("addAgentAliasesRequest", addAgentAliasesRequest),
    ("removeAgentAliasesRequest", removeAgentAliasesRequest),
    ("getAgentAliasesRequest", getAgentAliasesRequest),
    ("getDefaultAliasRequest", getDefaultAliasRequest),
    ("setDefaultAliasRequest", setDefaultAliasRequest),
    // Aliases
    ("addAliasExternalIdentitiesRequest", addAliasExternalIdentitiesRequest),
    ("removeAliasExternalIdentitiesRequest", removeAliasExternalIdentitiesRequest),
    ("getAliasExternalIdentitiesRequest", getAliasExternalIdentitiesRequest),
    ("setAliasDefaultExternalIdentityRequest", setAliasDefaultExternalIdentityRequest),
    // Connections
    ("removeAliasConnectionsRequest", removeAliasConnectionsRequest),
    ("getAliasConnectionsRequest", getAliasConnectionsRequest),
    ("getConnectionProfiles", getConnectionProfiles),
    // Alias Labels
    ("addAliasLabelsRequest", addAliasLabelsRequest),
    ("updateAliasLabelsRequest", updateAliasLabelsRequest),
    ("getAliasLabelsRequest", getAliasLabelsRequest),
    ("setAliasDefaultLabelRequest", setAliasDefaultLabelRequest),
    ("getAliasDefaultLabelRequest", getAliasDefaultLabelRequest),
    // System Labels
    //("addSystemLabelsRequest", addSystemLabelsRequest),
    // DSL
    ("evalSubscribeRequest", evalSubscribeRequest),
    ("evalSubscribeCancelRequest", evalSubscribeCancelRequest),
    // Introduction Protocol
    ("beginIntroductionRequest", beginIntroductionRequest),
    ("introductionConfirmationRequest", introductionConfirmationRequest),
    ("establishConnectionRequest", establishConnectionRequest),
    // Database dump/restore
    ("backupRequest", backupRequest),
    ("restoreRequest", restoreRequest),
    ("resetDatabaseRequest", resetDatabaseRequest),
    // Verifier protocol
    ("initiateClaim", initiateClaim),
    // omni
    ("omniGetBalance", omniGetBalance),
    ("omniTransfer", omniTransfer),
    ("setAmpWalletAddress", omniSetAmpWalletAddress)
    )

  @transient
  val apiMethods = HashMap[String, (JObject, ActorRef) => Unit](
    ("getAmpWalletAddress", omniGetAmpWalletAddress)
  )



  @transient
  val myRoute =
    cors {
      path("api") {
        post {
          decodeRequest(NoEncoding) {
            entity(as[String]) { jsonStr =>
              (ctx) => {
                try {
                  //BasicLogService.tweet("json: " + jsonStr)
                  val json = parse(jsonStr)
                  val msgType = (json \ "msgType").extract[String]
                  val msgId = (json \ "msgId").extractOpt[String]
                  val content = (json \ "content").extract[JObject]
                  syncMethods.get(msgType) match {
                    case Some(fn) => {
                      val key = UUID.randomUUID.toString
                      CompletionMapper.map += (key -> ctx)
                      fn(content, key)
                    }
                    case None => (content \ "sessionURI").extractOpt[String] match {
                      case None => {
                        ctx.complete(StatusCodes.Forbidden, "missing sessionURI parameter")
                      }
                      case Some(sessionURI) => {
                        CometActorMapper.map.get(sessionURI) match {
                          case None => {
                            ctx.complete(StatusCodes.Forbidden)
                          }
                          case Some(cometActor) => msgType match {
                            case "sessionPing" => {
                              cometActor ! SessionPing(ctx)
                            }
                            case "startSessionRecording" => cometActor ! StartCamera(ctx)
                            case "stopSessionRecording" => cometActor ! StopCamera(ctx)
                            case "closeSessionRequest" => cometActor ! CloseSession(Some(ctx))
                            case _ => {
                              cometActor ! KeepAlive()
                              apiMethods.get(msgType) match {
                                case Some(fn) => {
                                  cometActor ! RunApiFunction(fn, msgType, msgId, content)
                                  ctx.complete(StatusCodes.OK)
                                }
                                case _ => {
                                  asyncMethods.get(msgType) match {
                                    case Some(fn) => {
                                      cometActor ! RunFunction(fn, msgType, msgId, content)
                                      ctx.complete(StatusCodes.OK)
                                    }
                                    case _ => {
                                      ctx.complete(HttpResponse(500, "Unknown message type: " + msgType + "\n"))
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                catch {
                  case th: Throwable => {
                    val writer: java.io.StringWriter = new java.io.StringWriter()
                    val printWriter: java.io.PrintWriter = new java.io.PrintWriter(writer)
                    th.printStackTrace(printWriter)
                    printWriter.flush()

                    val stackTrace: String = writer.toString()
                    //println( "Malformed request: \n" + stackTrace )
                    BasicLogService.tweet("Malformed request: \n" + stackTrace)
                    ctx.complete(HttpResponse(500, "Malformed request: \n" + stackTrace))
                  }
                }
              }
            } // jsonStr
          } // decodeRequest
        } // post
      } ~
      pathPrefix("agentui") {
        getFromDirectory("./agentui")
      }

    }
}

/*
        path("admin/connectServers") {
          // allow administrators to make
          // sure servers are connected
          // BUGBUG : lgm -- make this secure!!!
          get {
            parameters('whoAmI) {
              (whoAmI: String) =>
                {
                  //             println(
                  //               (
                  //                 " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "
                  //                 + "in admin/connectServers2 "
                  //                 + " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "
                  //               )
                  //             )
                  BasicLogService.tweet(
                    (
                      " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "
                      + "in admin/connectServers2 "
                      + " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "))

                  connectServers("evaluator-service", UUID.randomUUID)

                  (cometActor ! SessionPing("", _))
                }
            }
          }
        } ~
        pathPrefix("splicious/btc") {
          get {
            parameters('btcToken) {
              (btcToken: String) =>
                {
                  BasicLogService.tweet(
                    (
                      " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "
                      + "in splicious/btc "
                      + " >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> "))

                  handleBTCResponse(btcToken)

                  (cometActor ! SessionPing("", _))

                }
            }
          }
        }
  */
