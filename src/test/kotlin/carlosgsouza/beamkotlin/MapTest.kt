package carlosgsouza.beamkotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Count.globally
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TypeDescriptors.*
import org.junit.After
import org.junit.Test

internal class KTransformTest {

    val pipeline: Pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false)

    @After
    fun runPipeline() {
        pipeline.run()
    }

    @Test
    fun testMap() {
        val input = pipeline.apply(Create.of(listOf(1, 2, 3)))

        val result = input.map(into = integers()) { 2 * it }

        PAssert.that(result).containsInAnyOrder(2, 4, 6)
    }

    @Test
    fun testFilter() {
        val input = pipeline.apply(Create.of(listOf(1, 2, 3)))

        val result = input.filter { it % 2 == 1 }

        PAssert.that(result).containsInAnyOrder(1, 3)
    }

    @Test
    fun testGlobalCount() {
        val input = pipeline.apply(Create.of(listOf("a", "b", "c", "c")))
        val globalCount = input.count(globally())
        PAssert.that(globalCount).containsInAnyOrder(4)
    }

    @Test
    fun testGroupByKey() {
        val input = pipeline.apply(
            Create.of(
                listOf(
                    KV.of("a", "Australia"),
                    KV.of("a", "Albania"),
                    KV.of("b", "Brazil"),
                    KV.of("c", "Canada"),
                    KV.of("c", "Chile"),
                )
            )
        )

        val result = input
            .groupByKey()
            // We convert the groupByKey result to a string built from the sorted values so we don't need to worry about
            // the order of the values Iterable in the assertion below.
            .map(into = strings()) {
                val countryCSV = it.value.toList().sorted().joinToString(",")
                "${it.key}: $countryCSV"
            }

        PAssert.that(result).containsInAnyOrder(
            "a: Albania,Australia",
            "b: Brazil",
            "c: Canada,Chile"
        )
    }

    @Test
    fun testChain() {
        val input = pipeline.apply(Create.of((1..100).toList()))

        val result = input
            .filter { it % 2 == 0 }
            .map(name = "Element+1000", into = integers()) { it + 1000 }
            .map(name = "KV(LastDigit, Element)", into = kvs(integers(), integers())) { KV.of(it % 10, it) }
            .groupByKey()
            .count(globally())

        PAssert.that(result).containsInAnyOrder(5)
    }


}