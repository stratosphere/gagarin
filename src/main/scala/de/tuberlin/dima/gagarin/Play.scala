package de.tuberlin.dima.gagarin

import eu.stratosphere.scala.{TextFile, ScalaPlan}
import eu.stratosphere.scala.operators.DelimitedDataSinkFormat
import eu.stratosphere.pact.client.LocalExecutor

object Play {

  def main(args: Array[String]) {

    val plan = new SomePlan().getPlan("/home/ssc/Entwicklung/datasets/movielens1M/ratings.dat", "/tmp/ozone/")

    plan.setDefaultParallelism(2)

    LocalExecutor.execute(plan)
    System.exit(0)
  }

}

case class Interaction(user: Int, item: Int)

class SomePlan extends Serializable {

  def formatOutput = (item: Int, count: Int) => "%d %d".format(item, count)

  def getPlan(inputPath: String, output: String): ScalaPlan = {

    val input = TextFile(inputPath)

    val itemInteractions = input.map({ line => { (line.split("::")(0).toInt, 1) }})

    val interactionsPerItem = itemInteractions.groupBy(_._1).reduce({ case (a, b) => (a._1, a._2 + b._2) })

    val sink = interactionsPerItem.write(output, DelimitedDataSinkFormat(formatOutput.tupled))

    new ScalaPlan(Seq(sink), "SomePlan")
  }

}