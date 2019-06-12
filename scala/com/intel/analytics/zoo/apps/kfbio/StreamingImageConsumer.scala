/*
 * Copyright 2018 Analytics Zoo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.analytics.zoo.apps.kfbio

import com.intel.analytics.bigdl.nn.Module
import com.intel.analytics.bigdl.tensor.Tensor

import com.intel.analytics.bigdl.numeric.NumericFloat

import com.intel.analytics.zoo.common.NNContext

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scopt.OptionParser
import com.intel.analytics.zoo.apps.kfbio.utils.ImageProcessing


case class RedisParams(modelPath: String = "",
                       weight: String = "",
                       defPath: String = "",
                       batchSize: Int = 4,
                       isInt8: Boolean = false,
                       topN: Int = 5)

object StreamingImageConsumer {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("breeze").setLevel(Level.ERROR)
  Logger.getLogger("com.intel.analytics.zoo.feature.image").setLevel(Level.ERROR)
  Logger.getLogger("com.intel.analytics.zoo").setLevel(Level.INFO)

  val parser = new OptionParser[RedisParams]("Redis Streaming Test") {
    opt[String]('d', "defPath")
      .text("folder that used to store the streaming paths")
      .action((x, c) => c.copy(defPath = x))
      .required()
    opt[String]('m', "model")
      .text("The path to the int8 quantized ResNet50 model snapshot")
      .action((v, p) => p.copy(modelPath = v))
      .required()
    //      opt[String]('w', "weight")
    //        .text("The path to the int8 ResNet50 model weight")
    //        .action((v, p) => p.copy(weight = v))
    opt[Int]('b', "batchSize")
      .text("Batch size of input data")
      .action((v, p) => p.copy(batchSize = v))
    opt[Int]("topN")
      .text("top N number")
      .action((v, p) => p.copy(topN = v))
    opt[Boolean]("isInt8")
      .text("Is Int8 optimized model?")
      .action((v, p) => p.copy(isInt8 = v))
  }

  val logger: Logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val param = parser.parse(args, RedisParams()).get
    val sc = NNContext.initNNContext("Redis Streaming Test")

    val batchSize = param.batchSize
    val model = Module.loadCaffeModel[Float](param.defPath, param.modelPath)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val images = spark
      .readStream
      .format("redis")
      .option("stream.keys", "image_stream")
      .option("stream.read.batch.size", batchSize.toString)
      .schema(StructType(Array(
        StructField("id", StringType),
        StructField("path", StringType),
        StructField("image", StringType)
        //        StructField("label", StringType)
      )))
      .load()

    val query = images
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) => {
        logger.info(s"Get batch")
        val batchImage = batchDF.rdd.map { image =>
          val bytes = java.util
            .Base64.getDecoder.decode(image.getAs[String]("image"))

          val uri = image.getAs[String]("path")

          (uri, ImageProcessing.preprocessBytes(bytes))

          //            val path = image.getAs[String]("path")
          //            logger.info(s"image: ${path}")
          //            ImageFeature.apply(bytes, null, path)
        }
//        logger.info(s"process ended")

        //        val inputTensor = Tensor[Float](batchSize, 3, 224, 224)
        val rec = batchImage.map {
          b => {
            val output = model.forward(b._2.resize(1, 3, 224, 224))
            val tensor = output.toTensor[Float]

            logger.info(b._1, tensor.valueAt(1), tensor.valueAt(2))
            (b._1, tensor.valueAt(1), tensor.valueAt(2))
          }
        }.count()

        logger.info(s"predict ended")
        //        imageTensor.grouped(batchSize).flatMap { batch =>
        //          val size = batchImage
        //          (0 until size).foreach { i =>
        //            inputTensor.select(1, i + 1).copy(batch(i)._2)
        //          }
        //          val start = System.nanoTime()
        //          logger.info(s"Begin Predict")
        //          val output = model.forward(inputTensor).toTensor[Float]
        //          //        val res2 = res1
        //          val end = System.nanoTime()
        //          logger.info(s"elapsed ${(end - start) / 1e9} s")
        //          (0 until size).map { i =>
        //            (batch(i)._1, output.valueAt(i + 1, 1),
        //              output.valueAt(i + 1, 2))
        //
        //            logger.info(batch(i)._1, output.valueAt(i + 1, 1),
        //              output.valueAt(i + 1, 2))
        //          }
        //        }
      }
    }.start()
    query.awaitTermination()
  }
}




//
//        val imageSet = ImageSet.array(batchImage)
//        val inputs = imageSet ->
//          ImageBytesToMat(imageCodec = Imgcodecs.CV_LOAD_IMAGE_COLOR) ->
//          ImageResize(256, 256) ->
//          ImageCenterCrop(224, 224) ->
//          ImageMatToTensor(shareBuffer = false) ->
//          ImageSetToSample()
//        val batched = inputs.toDataSet() -> SampleToMiniBatch(param.batchSize)
//        val start = System.nanoTime()
//        val predicts = batched.toLocal()
//          .data(false).flatMap { miniBatch =>
//          val predict = if (param.isInt8) {
//            model.doPredictInt8(miniBatch
//              .getInput.toTensor.addSingletonDimension())
//          } else {
//            model.doPredict(miniBatch
//              .getInput.toTensor.addSingletonDimension())
//          }
//          predict.toTensor.squeeze.split(1).asInstanceOf[Array[Activity]]
//        }
//        // Add prediction into imageset
//        imageSet.array.zip(predicts.toIterable).foreach(tuple => {
//          tuple._1(ImageFeature.predict) = tuple._2
//        })
//        // Transform prediction into Labels and probs
//        val labelOutput = LabelOutput(LabelReader.apply("IMAGENET"))
//        val results = labelOutput(imageSet).toLocal().array
//
//        // Output results
//        results.foreach(imageFeature => {
//          logger.info(s"image: ${imageFeature.uri}, top ${param.topN}")
//          val classes = imageFeature("classes").asInstanceOf[Array[String]]
//          val probs = imageFeature("probs").asInstanceOf[Array[Float]]
//          for (i <- 0 until param.topN) {
//            logger.info(s"\t class: ${classes(i)}, credit: ${probs(i)}")
//          }
//        })
//
//        val latency = System.nanoTime() - start
//        logger.info(s"Predict latency is ${latency / 1e6} ms")
//      }.start()
//
//    query.awaitTermination()
//  }
//}
//}



//    parser.parse(args, RedisParams()).foreach { param =>
//      val sc = NNContext.initNNContext("Redis Streaming Test")
//
//      val batchSize = param.batchSize


//      val model = new InferenceModel(1)
//
//      if (param.isInt8) {
//        model.doLoadOpenVINOInt8(param.model, param.weight, param.batchSize)
//      } else {
//        model.doLoadOpenVINO(param.model, param.weight)
//      }


      // Spark Structured Streaming
//      val spark = SparkSession
//        .builder
//        .master("local[*]")
//        .config("spark.redis.host", "localhost")
//        .config("spark.redis.port", "6379")
//        .getOrCreate()
//
//      val images = spark
//        .readStream
//        .format("redis")
//        .option("stream.keys", "image_stream")
//        .option("stream.read.batch.size", batchSize.toString)
//        .schema(StructType(Array(
//          StructField("id", StringType),
//          StructField("path", StringType),
//          StructField("image", StringType),
//          StructField("label", StringType)
//        )))
//        .load()
//
//      val query = images
//        .writeStream
//        .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
//          val batchImage = batchDF.collect().map { image =>
//            val bytes = java.util
//              .Base64.getDecoder.decode(image.getAs[String]("image"))
//
//
////            val path = image.getAs[String]("path")
////            logger.info(s"image: ${path}")
////            ImageFeature.apply(bytes, null, path)
//          }
//          val imageSet = ImageSet.array(batchImage)
//          val inputs = imageSet ->
//            ImageBytesToMat(imageCodec = Imgcodecs.CV_LOAD_IMAGE_COLOR) ->
//            ImageResize(256, 256) ->
//            ImageCenterCrop(224, 224) ->
//            ImageMatToTensor(shareBuffer = false) ->
//            ImageSetToSample()
//          val batched = inputs.toDataSet() -> SampleToMiniBatch(param.batchSize)
//          val start = System.nanoTime()
//          val predicts = batched.toLocal()
//            .data(false).flatMap { miniBatch =>
//            val predict = if (param.isInt8) {
//              model.doPredictInt8(miniBatch
//                .getInput.toTensor.addSingletonDimension())
//            } else {
//              model.doPredict(miniBatch
//                .getInput.toTensor.addSingletonDimension())
//            }
//            predict.toTensor.squeeze.split(1).asInstanceOf[Array[Activity]]
//          }
//          // Add prediction into imageset
//          imageSet.array.zip(predicts.toIterable).foreach(tuple => {
//            tuple._1(ImageFeature.predict) = tuple._2
//          })
//          // Transform prediction into Labels and probs
//          val labelOutput = LabelOutput(LabelReader.apply("IMAGENET"))
//          val results = labelOutput(imageSet).toLocal().array
//
//          // Output results
//          results.foreach(imageFeature => {
//            logger.info(s"image: ${imageFeature.uri}, top ${param.topN}")
//            val classes = imageFeature("classes").asInstanceOf[Array[String]]
//            val probs = imageFeature("probs").asInstanceOf[Array[Float]]
//            for (i <- 0 until param.topN) {
//              logger.info(s"\t class: ${classes(i)}, credit: ${probs(i)}")
//            }
//          })
//
//          val latency = System.nanoTime() - start
//          logger.info(s"Predict latency is ${latency / 1e6} ms")
//        }.start()
//
//      query.awaitTermination()
//    }
//  }
//}
