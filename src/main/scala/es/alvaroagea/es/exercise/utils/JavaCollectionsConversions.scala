package es.alvaroagea.es.exercise.utils

import java.util.Comparator
import java.util.function.BiConsumer
import java.util.function.Consumer
import java.util.function.Function
import java.util.function.Predicate

/**
 * Conversions between scala functions and Java 8 stream related functions.
 */
object JavaCollectionsConversions {

  /**
   * Transform a given scala function into a Java one.
   * @param f The scala function.
   * @tparam A Input generic type.
   * @tparam B Output generic type.
   * @return A Java function.
   */
  implicit def scalaToJavaFunction[A, B](f: (A) => B): Function[A, B] = {
    new Function[A, B] {
      override def apply(t: A): B = f(t)
    }
  }

  /**
   * Transform a given scala function into a Java Consumer.
   * @param f The scala function.
   * @tparam A Input generic type.
   * @tparam B Output parameter of the Scala function. The returning value is ignored.
   * @return A Java Consumer.
   */
  implicit def scalaToJavaConsumer[A, B](f: A => B): Consumer[A] = {
    new Consumer[A]() {
      override def accept(t: A): Unit = f(t)
    }
  }

  /**
   * Transform a given scala function into a Java Predicate.
   * @param f The scala function.
   * @tparam A Input generic type.
   * @return A Java Predicate.
   */
  implicit def scalaToJavaPredicate[A](f: A => Boolean): Predicate[A] = {
    new Predicate[A] {
      override def test(t: A): Boolean = f(t)
    }
  }

  /**
   * Transform a given scala function into a Java Comparator.
   * @param f The scala function.
   * @tparam A The input generic type.
   * @return A Java Comparator.
   */
  implicit def scalaToJavaComparator[A](f: (A, A) => Int): Comparator[A] = {
    new Comparator[A] {
      override def compare(o1: A, o2: A): Int = f(o1, o2)
    }
  }

  /**
   * Transform a given scala function into a Java BiConsumer
   * @param f The scala function.
   * @tparam A Input generic type.
   * @tparam B Input generic type.
   * @tparam C Output parameter of the Scala function. The returning value is ignored.
   * @return A Java BiConsumer.
   */
  implicit def scalaToJavaBiConsumer[A, B, C](f: (A, B) => C): BiConsumer[A, B] = {
    new BiConsumer[A, B]() {
      override def accept(t: A, d: B): Unit = f(t, d)
    }
  }


}
