package de.tuberlin.dima.gagarin.recommendation

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.scala.{TextFile, ScalaPlan}
import java.util.Random
import scala.collection.mutable

import eu.stratosphere.scala.operators._

object RunCooccurrenceAnalysis {

  def main(args: Array[String]) {

    val plan = new CooccurrenceAnalysis().getPlan("/home/ssc/Entwicklung/datasets/movielens1M/ratings.dat", "::",
                                                  "/tmp/ozone/")

    plan.setDefaultParallelism(2)

    LocalExecutor.execute(plan)
    System.exit(0)
  }

}

case class Interaction(user: Int, item: Int)

case class Cooccurrence(itemA: Int, itemB: Int, var count: Long) {

  def add(additional: Long) = {
    count += additional
    this
  }
}

case class SimilarItems(itemA: Int, itemB: Int, similarity: Double) {
  def mirror = {
    SimilarItems(itemB, itemA, similarity)
  }
}

class CooccurrenceAnalysis extends Serializable {

  def formatOutput = (itemPair: (Int,Int)) => { "%d %d".format(itemPair._1, itemPair._2) }


  def enumerateCooccurrences(interactions : Iterator[Interaction]): List[Cooccurrence] = {
    val i = interactions.toList
    for (interactionA <- i; interactionB <- i; if interactionA.item > interactionB.item)
      yield { Cooccurrence(interactionA.item, interactionB.item, 1l) }
  }

  def getPlan(inputPath: String, separator: String, output: String): ScalaPlan = {

    val maxInteractionsPerUserOrItem = 500
    val maxSimilarItemsPerItem = 100

    val interactions = TextFile(inputPath).map({ line => {
      val fields = line.split(separator)
      Interaction(fields(0).toInt, fields(1).toInt)
    }})

    val numInteractionsPerItem = interactions.map({ interaction => (interaction.item, 1l) })
      .groupBy(_._1)
      .reduce({ case ((item, countA), (_, countB)) => (item, countA + countB) })

    val numInteractionsPerUser = interactions.map({ interaction => (interaction.user, 1l) })
      .groupBy(_._1)
      .reduce({ case ((user, countA), (_, countB)) => (user, countA + countB) })

    val downsampledInteractions = interactions.join(numInteractionsPerItem).where(_.item).isEqualTo(_._1)
      .map({ case (interaction, (_, numItemInteractions)) =>
        ((interaction), numItemInteractions)
      })
      .join(numInteractionsPerUser).where(_._1.user).isEqualTo(_._1)
      .filter({ case ((interaction, numItemInteractions), (_, numUserInteractions)) => {

        val perUserSampleRate =
          math.min(maxInteractionsPerUserOrItem, numUserInteractions) / numUserInteractions
        val perItemSampleRate =
          math.min(maxInteractionsPerUserOrItem, numItemInteractions) / numItemInteractions

        new Random().nextDouble() <= math.min(perUserSampleRate, perItemSampleRate)
      }}).map({ case ((interaction, _), _) => interaction })

    val cooccurrences = downsampledInteractions.groupBy(_.user).groupReduce( enumerateCooccurrences ).flatMap ({ _.iterator })
      .groupBy(cooccurrence => (cooccurrence.itemA, cooccurrence.itemB)).reduce({ (cooccurrence1, cooccurrence2) => cooccurrence1.add(cooccurrence2.count) })

    val numInteractions = downsampledInteractions.map({ _ => (1l, 1l) }).groupBy(_._1).reduce({ case ((_, countA), (_, countB)) => (1l, countA + countB) })


    val similarities = cooccurrences.join(numInteractionsPerItem).where(_.itemA).isEqualTo(_._1)
      .map({ case (cooccurrence, (_, numItemInteractionsWithItemA)) =>
        (cooccurrence, numItemInteractionsWithItemA)
      }).join(numInteractionsPerItem).where(_._1.itemB).isEqualTo(_._1)
      .map({ case ((cooccurrence, numInteractionsWithItemA), (_, numInteractionsWithItemB)) => {
        (cooccurrence, numInteractionsWithItemA, numInteractionsWithItemB)
      }})
      .cross(numInteractions).map({ case ((cooccurrence, numInteractionsWithItemA, numInteractionsWithItemB), (_, numInteractions)) => {

        val interactionsWithAandB = cooccurrence.count
        val interactionsWithAnotB = numInteractionsWithItemA - interactionsWithAandB
        val interactionsWithBnotA = numInteractionsWithItemA - interactionsWithAandB
        val interactionsWithNeitherAnorB = numInteractions - numInteractionsWithItemA -
          numInteractionsWithItemB + interactionsWithAandB

        val logLikelihood = LogLikelihood.logLikelihoodRatio(interactionsWithAandB, interactionsWithAnotB,
          interactionsWithBnotA, interactionsWithNeitherAnorB)
        val logLikelihoodSimilarity = 1.0 - 1.0 / (1.0 + logLikelihood)

        SimilarItems(cooccurrence.itemA, cooccurrence.itemB, logLikelihoodSimilarity)
      }})


    val bidirectionalSimilarities = similarities.flatMap { similarItems =>
      Seq(similarItems, similarItems.mirror) }

    val order = Ordering.fromLessThan[SimilarItems]({ case (similarItemsA, similarItemsB) => {
      similarItemsA.similarity > similarItemsB.similarity
    }})

    /* use a fixed-size priority queue to only retain the top similar items per item */
    val topKSimilarities = bidirectionalSimilarities.groupBy(_.itemA).groupReduce({ case candidates => {

      val queue = new mutable.PriorityQueue[SimilarItems]()(order)

      candidates.foreach({ candidateSimilarItems => {
        if (queue.size < maxSimilarItemsPerItem) {
          queue.enqueue(candidateSimilarItems)
        } else {
          if (order.lt(candidateSimilarItems, queue.head)) {
            queue.dequeue()
            queue.enqueue(candidateSimilarItems)
          }
        }
      }})

      for (similarItems <- queue.dequeueAll)
        yield { (similarItems.itemA, similarItems.itemB) }
    }}).flatMap(_.iterator)


    val sink = topKSimilarities.write(output, DelimitedDataSinkFormat(formatOutput))

    new ScalaPlan(Seq(sink), "SomePlan")
  }
}

object LogLikelihood {

  def logLikelihoodRatio(k11: Long, k12: Long, k21: Long, k22: Long) = {
    val rowEntropy: Double = entropy(k11 + k12, k21 + k22)
    val columnEntropy: Double = entropy(k11 + k21, k12 + k22)
    val matrixEntropy: Double = entropy(k11, k12, k21, k22)
    if (rowEntropy + columnEntropy < matrixEntropy) {
      0.0
    } else {
      2.0 * (rowEntropy + columnEntropy - matrixEntropy)
    }
  }

  private def xLogX(x: Long): Double = {
    if (x == 0) {
      0.0
    } else {
      x * math.log(x)
    }
  }

  private def entropy(a: Long, b: Long): Double = { xLogX(a + b) - xLogX(a) - xLogX(b) }

  private def entropy(elements: Long*): Double = {
    var sum: Long = 0
    var result: Double = 0.0
    for (element <- elements) {
      result += xLogX(element)
      sum += element
    }
    xLogX(sum) - result
  }
}