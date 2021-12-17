package carlosgsouza.beamkotlin

import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor

/**
 * Extension functions for PCollections that make it easier to work with them in Kotlin.
 */

fun <K, V> PCollection<KV<K, V>>.groupByKey(): PCollection<KV<K, Iterable<V>>> {
    return this.apply(GroupByKey.create())
}

fun <T, O> PCollection<T>.count(mode: PTransform<PCollection<T>, PCollection<O>>): PCollection<O> {
    return this.apply(mode)
}

fun <T> PCollection<T>.filter(by: (T) -> Boolean): PCollection<T> {
    return this.apply(Filter.by(SerializableFunction { by(it) }))
}

fun <I, O> PCollection<I>.map(into: TypeDescriptor<O>, name: String? = null, via: (I) -> O): PCollection<O> {
    val mapElements = MapElements
        .into(into)
        .via<I>(SerializableFunction { via(it) })

    return if (name != null) apply(name, mapElements) else apply(mapElements)
}