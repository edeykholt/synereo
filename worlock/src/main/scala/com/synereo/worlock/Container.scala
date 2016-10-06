package com.synereo.worlock

import java.io.File
import java.nio.file.Paths

import com.biosimilarity.evaluator.BuildInfo
import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.command.CreateContainerResponse
import com.github.dockerjava.api.model.Ports.Binding
import com.github.dockerjava.api.model._
import com.github.dockerjava.core.command.BuildImageResultCallback
import com.github.dockerjava.core.{
  DefaultDockerClientConfig,
  DockerClientBuilder
}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.Try

trait Container {

  val logger: Logger = LoggerFactory.getLogger(classOf[Container])

  val defaultDockerClientConfig: DefaultDockerClientConfig =
    DefaultDockerClientConfig
      .createDefaultConfigBuilder()
      .withDockerHost("tcp://localhost:2376")
      .build()

  val defaultBuildImageResultCallback = new BuildImageResultCallback() {
    override def onNext(item: BuildResponseItem): Unit = {
      logger.info(item.getStream.replaceAll("\n", ""))
      super.onNext(item)
    }
  }

  val baseImagePath: File = Paths
    .get(System.getProperty("user.dir"))
    .resolve("src/main/docker/base")
    .toFile

  val colocatedEnvironment = Try {
    val localhostIpAddress = "127.0.0.1"
    val localhostRabbitPort = "5672"
    Map[String, String](
      "DEPLOYMENT_MODE" -> "colocated",
      "DSL_COMM_LINK_CLIENT_HOSTS" -> s"$localhostIpAddress:$localhostRabbitPort",
      "DSL_EVALUATOR_HOST" -> localhostIpAddress,
      "DSL_EVALUATOR_PORT" -> localhostRabbitPort,
      "DSL_EVALUATOR_PREFERRED_SUPPLIER_HOST" -> localhostIpAddress,
      "DSL_EVALUATOR_PREFERRED_SUPPLIER_PORT" -> localhostRabbitPort,
      "BFACTORY_COMM_LINK_SERVER_HOST" -> localhostIpAddress,
      "BFACTORY_COMM_LINK_SERVER_PORT" -> localhostRabbitPort,
      "BFACTORY_COMM_LINK_CLIENT_HOST" -> localhostIpAddress,
      "BFACTORY_COMM_LINK_CLIENT_PORT" -> localhostRabbitPort,
      "BFACTORY_EVALUATOR_HOST" -> localhostIpAddress,
      "BFACTORY_EVALUATOR_PORT" -> localhostRabbitPort)
  }

  val tcp9876 = ExposedPort.tcp(9876)

  val colocatedPortBindings: Try[Ports] = Try {
    val ports = new Ports()
    ports.bind(tcp9876, Binding.bindPort(9876))
    ports
  }

  private def environmentMapToList(env: Map[String, String]): List[String] =
    env.foldLeft(List.empty[String]) { (accum: List[String], curr: (String, String)) =>
      s"${curr._1}=${curr._2}" :: accum
    }

  def getDockerClient(config: DefaultDockerClientConfig = defaultDockerClientConfig): Try[DockerClient] =
    Try(DockerClientBuilder.getInstance(config).build())

  def buildContainer(client: DockerClient,
                     dir: File,
                     callback: BuildImageResultCallback =
                       defaultBuildImageResultCallback): Try[String] =
    Try(client.buildImageCmd(dir).exec(callback).awaitImageId())

  def createContainer(client: DockerClient,
                      name: String,
                      environment: Map[String, String],
                      binds: Ports): Try[CreateContainerResponse] =
    Try {
      client
        .createContainerCmd(s"gloseval:${BuildInfo.version}")
        .withName(name)
        .withEnv(environmentMapToList(environment))
        .withPortBindings(binds)
        .exec()
    }

  def startContainer(client: DockerClient, containerId: String): Try[Unit] =
    Try(client.startContainerCmd(containerId).exec())

  def colocatedTest(): Try[(DockerClient, CreateContainerResponse)] =
    for {
      client      <- getDockerClient()
      environment <- colocatedEnvironment
      bindings    <- colocatedPortBindings
      container   <- createContainer(client, "envtest", environment, bindings)
      _           <- startContainer(client, container.getId)
    } yield (client, container)
}
