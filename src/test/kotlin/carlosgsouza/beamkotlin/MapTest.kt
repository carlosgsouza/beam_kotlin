package carlosgsouza.beamkotlin

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.Count.globally
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors
import org.apache.beam.sdk.values.TypeDescriptors.integers
import org.junit.After
import org.junit.Rule
import org.junit.Test


internal class MapTest {

    val pipeline: Pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false)

    @After
    fun runPipeline() {
        pipeline.run()
    }

    @Test
    fun testMap() {
        val input = pipeline.apply(Create.of(listOf(1, 2, 3)))

        val result = input.map(into = integers()) {2 * it}

        PAssert.that(result).containsInAnyOrder(2, 4, 6)
    }

    @Test
    fun testFilter() {
        val input = pipeline.apply(Create.of(listOf(1, 2, 3)))

        val result = input.filter { it % 2 == 1}

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
        val input = pipeline.apply(Create.of(listOf(
//            KV.of("a", "Australia"),
            KV.of("a", "Albania"),
            KV.of("b", "Brazil"),
            KV.of("c", "Canada"),
//            KV.of("c", "Chile"),
        )))

        val result = input.groupByKey()

        // TODO: Test multiple values in iterable once I understand how to accept values in any order. Maybe use
        // PAssert.satisfy?
        PAssert.that(result).containsInAnyOrder(
            KV.of("a", listOf("Albania")),
            KV.of("b", listOf("Brazil")),
            KV.of("c", listOf("Canada"))
        )
    }

    @Test
    fun testChain() {
        val input = pipeline.apply(Create.of(listOf(1, 2, 3)))

        val result = input.map(into = integers()) { it + 1 }.filter { it % 2 == 0 }.count(globally())

        PAssert.that(result).containsInAnyOrder(2)
    }

    fun <K, V> PCollection<KV<K, V>>.groupByKey(): PCollection<KV<K, Iterable<V>>> {
        return this.apply(GroupByKey.create())
    }

    fun <T, O> PCollection<T>.count(mode : PTransform<PCollection<T>, PCollection<O>>) : PCollection<O> {
        return this.apply(mode)
    }

    fun <T> PCollection<T>.filter(by: (T) -> Boolean) : PCollection<T> {
        return this.apply(Filter.by( SerializableFunction { by(it) }))
    }

    fun <I, O> PCollection<I>.map(into: TypeDescriptor<O>, via: (I) -> O): PCollection<O> {
        return this.apply(
            MapElements
                .into(into)
                .via(SerializableFunction { via(it) })
        )
    }
}